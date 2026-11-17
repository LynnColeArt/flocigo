// Package partiql implements the PartiQL statement surface of the local
// DynamoDB-compatible store: a lexer and recursive-descent parser for
// SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE statements,
// DynamoDB-style condition and value evaluation, key-condition planning that
// routes statements to an exact partition (or a single item), and the
// execution engine on top of a table store.
//
// Ports of the floci sources: DynamoDbPartiQLParser, DynamoDbPartiQLHandler,
// DynamoDbPartiQLKeyPlan, ExpressionEvaluator, DynamoDbUpdateExpressionSyntax
// and DynamoDbConditionKeys.
package partiql

import (
  "bytes"
  "fmt"
  "math/big"
  "strings"
)

// Error codes surfaced by the PartiQL engine (DynamoDB API error codes).
const (
  CodeValidation = "ValidationException"
  CodeNotFound   = "ResourceNotFoundException"
  CodeInUse      = "ResourceInUseException"
  CodeDuplicate  = "DuplicateItemException"
)

// Error is an engine error carrying a DynamoDB API error code.
type Error struct {
  Code string
  Msg  string
}

func (e *Error) Error() string {
  if e == nil {
    return "<nil>"
  }
  return e.Code + ": " + e.Msg
}

func errValidation(format string, args ...interface{}) *Error {
  return &Error{Code: CodeValidation, Msg: fmt.Sprintf(format, args...)}
}

func errNotFound(format string, args ...interface{}) *Error {
  return &Error{Code: CodeNotFound, Msg: fmt.Sprintf(format, args...)}
}

// AttrValue is a typed DynamoDB attribute value.
type AttrValue struct {
  Type string // S, N, B, SS, NS, BS, M, L, BOOL, NULL
  S    string
  N    string
  B    []byte
  SS   []string
  NS   []string
  BS   [][]byte
  M    map[string]*AttrValue
  L    []*AttrValue
  BOOL bool
}

func SVal(s string) *AttrValue     { return &AttrValue{Type: "S", S: s} }
func NVal(n string) *AttrValue     { return &AttrValue{Type: "N", N: n} }
func BVal(b []byte) *AttrValue     { return &AttrValue{Type: "B", B: copyBytes(b)} }
func SSVal(xs []string) *AttrValue { return &AttrValue{Type: "SS", SS: copyStrs(xs)} }
func NSVal(xs []string) *AttrValue { return &AttrValue{Type: "NS", NS: copyStrs(xs)} }
func BSVal(xs [][]byte) *AttrValue { return &AttrValue{Type: "BS", BS: copyByteSet(xs)} }
func MVal(m map[string]*AttrValue) *AttrValue {
  return &AttrValue{Type: "M", M: m}
}
func LVal(l []*AttrValue) *AttrValue { return &AttrValue{Type: "L", L: l} }
func BoolVal(b bool) *AttrValue      { return &AttrValue{Type: "BOOL", BOOL: b} }
func NullVal() *AttrValue            { return &AttrValue{Type: "NULL"} }

func copyBytes(b []byte) []byte {
  if b == nil {
    return nil
  }
  return append([]byte(nil), b...)
}

func copyStrs(xs []string) []string {
  if xs == nil {
    return nil
  }
  return append([]string(nil), xs...)
}

func copyByteSet(xs [][]byte) [][]byte {
  if xs == nil {
    return nil
  }
  out := make([][]byte, len(xs))
  for i, b := range xs {
    out[i] = copyBytes(b)
  }
  return out
}

// Clone deep-copies the value.
func (v *AttrValue) Clone() *AttrValue {
  if v == nil {
    return nil
  }
  c := &AttrValue{Type: v.Type, S: v.S, N: v.N, BOOL: v.BOOL}
  c.B = copyBytes(v.B)
  c.SS = copyStrs(v.SS)
  c.NS = copyStrs(v.NS)
  c.BS = copyByteSet(v.BS)
  if v.M != nil {
    c.M = make(map[string]*AttrValue, len(v.M))
    for k, m := range v.M {
      c.M[k] = m.Clone()
    }
  }
  if v.L != nil {
    c.L = make([]*AttrValue, len(v.L))
    for i, e := range v.L {
      c.L[i] = e.Clone()
    }
  }
  return c
}

// Debug renders a compact, human readable value for error messages.
func (v *AttrValue) Debug() string {
  if v == nil {
    return "MISSING"
  }
  switch v.Type {
  case "S":
    return fmt.Sprintf("S(%q)", v.S)
  case "N":
    return fmt.Sprintf("N(%s)", v.N)
  case "B":
    return fmt.Sprintf("B(len=%d)", len(v.B))
  case "BOOL":
    return fmt.Sprintf("BOOL(%v)", v.BOOL)
  case "NULL":
    return "NULL"
  case "SS":
    return fmt.Sprintf("SS(len=%d)", len(v.SS))
  case "NS":
    return fmt.Sprintf("NS(len=%d)", len(v.NS))
  case "BS":
    return fmt.Sprintf("BS(len=%d)", len(v.BS))
  case "M":
    return fmt.Sprintf("M(len=%d)", len(v.M))
  case "L":
    return fmt.Sprintf("L(len=%d)", len(v.L))
  }
  return v.Type
}

// parseRat parses a DynamoDB number string exactly as a rational.
func parseRat(s string) (*big.Rat, bool) {
  r, ok := new(big.Rat).SetString(s)
  if !ok {
    return nil, false
  }
  return r, true
}

// CompareDecimal compares two number strings numerically and exactly.
func CompareDecimal(a, b string) (int, error) {
  af, ok := parseRat(a)
  if !ok {
    return 0, errValidation("invalid number %q", a)
  }
  bf, ok := parseRat(b)
  if !ok {
    return 0, errValidation("invalid number %q", b)
  }
  return af.Cmp(bf), nil
}

// decimalString renders a rational as a finite decimal string when possible;
// non-terminating values fall back to a high precision approximation.
func decimalString(r *big.Rat) string {
  den := r.Denom()
  twos, fives := 0, 0
  d := new(big.Int).Set(den)
  for {
    q, rem := new(big.Int).QuoRem(d, big.NewInt(2), new(big.Int))
    if rem.Sign() != 0 {
      break
    }
    d = q
    twos++
  }
  for {
    q, rem := new(big.Int).QuoRem(d, big.NewInt(5), new(big.Int))
    if rem.Sign() != 0 {
      break
    }
    d = q
    fives++
  }
  if d.Cmp(big.NewInt(1)) != 0 {
    f := new(big.Float).SetPrec(128).SetRat(r)
    return f.Text('f', -1)
  }
  digits := twos
  if fives > digits {
    digits = fives
  }
  scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil)
  q := new(big.Int).Quo(scale, den)
  num := new(big.Int).Mul(r.Num(), q)
  str := num.String()
  neg := strings.HasPrefix(str, "-")
  if neg {
    str = str[1:]
  }
  for len(str) <= digits {
    str = "0" + str
  }
  var out string
  if digits == 0 {
    out = str
  } else {
    out = str[:len(str)-digits] + "." + str[len(str)-digits:]
  }
  if neg {
    out = "-" + out
  }
  return out
}

// arith applies a numeric operator to two decimal operands.
func arith(a, b, op string) (string, error) {
  af, ok := parseRat(a)
  if !ok {
    return "", errValidation("invalid number %q", a)
  }
  bf, ok := parseRat(b)
  if !ok {
    return "", errValidation("invalid number %q", b)
  }
  var r *big.Rat
  switch op {
  case "+":
    r = new(big.Rat).Add(af, bf)
  case "-":
    r = new(big.Rat).Sub(af, bf)
  case "*":
    r = new(big.Rat).Mul(af, bf)
  case "/":
    if bf.Sign() == 0 {
      return "", errValidation("division by zero")
    }
    r = new(big.Rat).Quo(af, bf)
  case "%":
    if bf.Sign() == 0 {
      return "", errValidation("division by zero")
    }
    num := new(big.Int).Mul(af.Num(), bf.Denom())
    den := new(big.Int).Mul(af.Denom(), bf.Num())
    q := new(big.Int).Quo(num, den)
    r = new(big.Rat).Sub(af, new(big.Rat).Mul(new(big.Rat).SetInt(q), bf))
  default:
    return "", errValidation("unknown operator %q", op)
  }
  return decimalString(r), nil
}

// negateNumber flips the sign of a decimal literal.
func negateNumber(s string) string {
  if strings.HasPrefix(s, "-") {
    return s[1:]
  }
  return "-" + s
}

func hexKey(b []byte) string { return fmt.Sprintf("%x", b) }

// EqualAttr reports DynamoDB value equality (deep; sets order-insensitive).
func EqualAttr(a, b *AttrValue) bool {
  if a == nil || b == nil {
    return a == b
  }
  if a.Type != b.Type {
    return false
  }
  switch a.Type {
  case "S":
    return a.S == b.S
  case "N":
    c, err := CompareDecimal(a.N, b.N)
    return err == nil && c == 0
  case "B":
    return bytes.Equal(a.B, b.B)
  case "BOOL":
    return a.BOOL == b.BOOL
  case "NULL":
    return true
  case "SS":
    return equalSet(a.SS, b.SS, false)
  case "NS":
    return equalSet(a.NS, b.NS, true)
  case "BS":
    return equalByteSet(a.BS, b.BS)
  case "M":
    if len(a.M) != len(b.M) {
      return false
    }
    for k, av := range a.M {
      bv, ok := b.M[k]
      if !ok || !EqualAttr(av, bv) {
        return false
      }
    }
    return true
  case "L":
    if len(a.L) != len(b.L) {
      return false
    }
    for i := range a.L {
      if !EqualAttr(a.L[i], b.L[i]) {
        return false
      }
    }
    return true
  }
  return false
}

func equalSet(xs, ys []string, numeric bool) bool {
  if len(xs) != len(ys) {
    return false
  }
  used := make([]bool, len(ys))
  for _, x := range xs {
    match := false
    for j, y := range ys {
      if used[j] {
        continue
      }
      if numeric {
        if c, err := CompareDecimal(x, y); err == nil && c == 0 {
          used[j] = true
          match = true
          break
        }
      } else if x == y {
        used[j] = true
        match = true
        break
      }
    }
    if !match {
      return false
    }
  }
  return true
}

func equalByteSet(xs, ys [][]byte) bool {
  seenA := make(map[string]bool, len(xs))
  for _, x := range xs {
    seenA[hexKey(x)] = true
  }
  seenB := make(map[string]bool, len(ys))
  for _, y := range ys {
    seenB[hexKey(y)] = true
  }
  if len(seenA) != len(seenB) {
    return false
  }
  for k := range seenA {
    if !seenB[k] {
      return false
    }
  }
  return true
}

// CompareAttr orders values of the same comparable scalar type (S, N, B).
func CompareAttr(a, b *AttrValue) (int, error) {
  if a == nil || b == nil {
    return 0, errValidation("cannot order missing values")
  }
  if a.Type != b.Type {
    return 0, errValidation("type mismatch: %s vs %s", a.Type, b.Type)
  }
  switch a.Type {
  case "S":
    return strings.Compare(a.S, b.S), nil
  case "B":
    return bytes.Compare(a.B, b.B), nil
  case "N":
    return CompareDecimal(a.N, b.N)
  }
  return 0, errValidation("cannot order values of type %s", a.Type)
}

// CompareKeyOrder orders sort-key values (used for partition item ordering).
func CompareKeyOrder(a, b *AttrValue) int {
  if a == nil && b == nil {
    return 0
  }
  if a == nil {
    return 1
  }
  if b == nil {
    return -1
  }
  if a.Type != b.Type {
    return strings.Compare(a.Type, b.Type)
  }
  switch a.Type {
  case "N":
    c, _ := CompareDecimal(a.N, b.N)
    return c
  case "B":
    return bytes.Compare(a.B, b.B)
  }
  return strings.Compare(a.S, b.S)
}

// CanonicalValue renders a value deterministically (numbers are normalized so
// that "5" and "5.0" produce the same key).
func CanonicalValue(v *AttrValue) string {
  if v == nil {
    return "\x01missing"
  }
  switch v.Type {
  case "S":
    return "S:" + v.S
  case "N":
    if r, ok := parseRat(v.N); ok {
      return "N:" + decimalString(r)
    }
    return "N:" + v.N
  case "B":
    return "B:" + hexKey(v.B)
  }
  return v.Type + ":" + v.Debug()
}

// isKeyScalar reports whether v is a scalar usable as a key value.
func isKeyScalar(v *AttrValue) bool {
  return v != nil && (v.Type == "S" || v.Type == "N" || v.Type == "B")
}
