package partiql

import (
  "bytes"
  "fmt"
  "math/big"
  "strconv"
  "strings"
)

// This file consolidates the evaluator ports (floci ExpressionEvaluator and
// the condition-key helpers): DynamoDB-semantics condition evaluation for
// WHERE clauses and value evaluation for SET / INSERT / function arguments.

// evalCtx carries the state needed to evaluate expressions for one statement.
type evalCtx struct {
  params []*AttrValue
  named  map[string]*AttrValue
  alias  string
  item   map[string]*AttrValue
}

func (c *evalCtx) withItem(item map[string]*AttrValue) *evalCtx {
  nc := *c
  nc.item = item
  return &nc
}

// resolveBase maps a path to its effective root attribute and remaining
// steps, honoring alias qualification (FROM Music AS m: m.Artist = Artist).
func resolveBase(p *PathExpr, alias string) (string, []PathStep, error) {
  if alias != "" && p.Root == alias && len(p.Steps) > 0 {
    if p.Steps[0].IsIdx {
      return "", nil, errValidation("alias %q cannot be followed by a list index", alias)
    }
    return p.Steps[0].Key, p.Steps[1:], nil
  }
  return p.Root, p.Steps, nil
}

func pathString(p *PathExpr) string {
  var sb strings.Builder
  sb.WriteString(p.Root)
  for _, st := range p.Steps {
    if st.IsIdx {
      if st.IdxExpr != nil {
        sb.WriteString("[?]")
        continue
      }
      fmt.Fprintf(&sb, "[%d]", st.Index)
      continue
    }
    sb.WriteString(".")
    sb.WriteString(st.Key)
  }
  return sb.String()
}

// stepIndex resolves the integer index of a path step.
func stepIndex(c *evalCtx, st PathStep) (int, error) {
  if st.IdxExpr == nil {
    return st.Index, nil
  }
  v, err := evalValue(c, st.IdxExpr)
  if err != nil {
    return 0, err
  }
  return attrToInt(v)
}

// attrToInt converts a DynamoDB number to a non-negative int list index.
func attrToInt(v *AttrValue) (int, error) {
  if v == nil || v.Type != "N" {
    return 0, errValidation("list indexes must be numbers, found %s", v.Debug())
  }
  r, ok := parseRat(v.N)
  if !ok {
    return 0, errValidation("invalid number %q", v.N)
  }
  q := new(big.Int).Quo(r.Num(), r.Denom())
  exact := new(big.Rat).SetInt(q)
  if r.Cmp(exact) != 0 || r.Sign() < 0 {
    return 0, errValidation("list indexes must be non-negative integers")
  }
  if !q.IsInt64() {
    return 0, errValidation("list index out of range")
  }
  return int(q.Int64()), nil
}

// resolvePath resolves a path against the context item. Missing segments
// resolve to "not found" (DynamoDB condition semantics); traversal through a
// scalar or an out-of-range index simply misses.
func resolvePath(c *evalCtx, p *PathExpr) (*AttrValue, bool, error) {
  root, steps, err := resolveBase(p, c.alias)
  if err != nil {
    return nil, false, err
  }
  if c.item == nil {
    return nil, false, errValidation("path %q requires an item context", pathString(p))
  }
  cur, ok := c.item[root]
  if !ok || cur == nil {
    return nil, false, nil
  }
  for _, st := range steps {
    if cur == nil {
      return nil, false, nil
    }
    if st.IsIdx {
      if cur.Type != "L" {
        return nil, false, nil
      }
      idx, err := stepIndex(c, st)
      if err != nil {
        return nil, false, err
      }
      if idx < 0 || idx >= len(cur.L) {
        return nil, false, nil
      }
      cur = cur.L[idx]
      continue
    }
    if cur.Type != "M" {
      return nil, false, nil
    }
    nv, ok2 := cur.M[st.Key]
    if !ok2 {
      return nil, false, nil
    }
    cur = nv
  }
  if cur == nil {
    return nil, false, nil
  }
  return cur, true, nil
}

// gentleValue resolves a comparison operand: paths and MISSING literals may
// be absent (found=false); everything else evaluates strictly.
func gentleValue(c *evalCtx, e Expr) (*AttrValue, bool, error) {
  switch t := e.(type) {
  case *PathExpr:
    return resolvePath(c, t)
  case *LitExpr:
    if t.Missing {
      return nil, false, nil
    }
    return t.Val.Clone(), true, nil
  case *FuncExpr:
    if t.Name == "size" && len(t.Args) == 1 {
      if px, ok := t.Args[0].(*PathExpr); ok {
        v, found, err := resolvePath(c, px)
        if err != nil || !found {
          return nil, false, err
        }
        n, err := sizeOf(v, pathString(px))
        if err != nil {
          return nil, false, err
        }
        return n, true, nil
      }
    }
  }
  v, err := evalValue(c, e)
  if err != nil {
    return nil, false, err
  }
  return v, true, nil
}

// sizeOf implements the DynamoDB size() function.
func sizeOf(v *AttrValue, name string) (*AttrValue, error) {
  switch v.Type {
  case "S":
    return NVal(strconv.Itoa(len(v.S))), nil
  case "B":
    return NVal(strconv.Itoa(len(v.B))), nil
  case "SS", "NS", "BS", "L", "M":
    n := len(v.SS) + len(v.NS) + len(v.BS) + len(v.L) + len(v.M)
    return NVal(strconv.Itoa(n)), nil
  }
  return nil, errValidation("size() requires a string, binary, set, list, or map (at %s)", name)
}

// resolveParam binds a parameter reference.
func resolveParam(c *evalCtx, p *ParamExpr) (*AttrValue, error) {
  if p.Named {
    if v, ok := c.named[p.Name]; ok {
      return v.Clone(), nil
    }
    return nil, errValidation("no value provided for parameter :%s", p.Name)
  }
  if p.Index < 0 || p.Index >= len(c.params) {
    return nil, errValidation("no value provided for parameter at position %d", p.Index)
  }
  return c.params[p.Index].Clone(), nil
}

// evalValue evaluates an expression as a value (right-hand side context).
func evalValue(c *evalCtx, e Expr) (*AttrValue, error) {
  switch x := e.(type) {
  case *LitExpr:
    if x.Missing || x.Val == nil {
      return nil, errValidation("MISSING cannot be used as a value")
    }
    return x.Val.Clone(), nil
  case *ParamExpr:
    return resolveParam(c, x)
  case *PathExpr:
    v, ok, err := resolvePath(c, x)
    if err != nil {
      return nil, err
    }
    if !ok {
      return nil, errValidation("the provided expression refers to an attribute that does not exist in the item: %s", pathString(x))
    }
    return v.Clone(), nil
  case *MapLitExpr:
    m := make(map[string]*AttrValue, len(x.Lit.Fields))
    for _, f := range x.Lit.Fields {
      if _, dup := m[f.Key]; dup {
        return nil, errValidation("duplicate attribute %q in document literal", f.Key)
      }
      v, err := evalValue(c, f.Val)
      if err != nil {
        return nil, err
      }
      m[f.Key] = v
    }
    return MVal(m), nil
  case *ListLitExpr:
    var items []*AttrValue
    for _, it := range x.Items {
      v, err := evalValue(c, it)
      if err != nil {
        return nil, err
      }
      items = append(items, v)
    }
    return LVal(items), nil
  case *FuncExpr:
    return evalValueFunc(c, x)
  case *BinExpr:
    return evalValueBinary(c, x)
  case *UnaryExpr:
    if x.Op != "-" {
      return nil, errValidation("expression cannot be used as a value")
    }
    v, err := evalValue(c, x.X)
    if err != nil {
      return nil, err
    }
    if v.Type != "N" {
      return nil, errValidation("unary '-' requires a number, found %s", v.Type)
    }
    return NVal(negateNumber(v.N)), nil
  }
  return nil, errValidation("expression cannot be used as a value")
}

func evalValueBinary(c *evalCtx, x *BinExpr) (*AttrValue, error) {
  switch x.Op {
  case "+", "-", "*", "/", "%", "||":
  default:
    return nil, errValidation("operator %q cannot be used in a value context", x.Op)
  }
  l, err := evalValue(c, x.L)
  if err != nil {
    return nil, err
  }
  r, err := evalValue(c, x.R)
  if err != nil {
    return nil, err
  }
  if x.Op == "||" {
    if l.Type == "S" && r.Type == "S" {
      return SVal(l.S + r.S), nil
    }
    if l.Type == "L" && r.Type == "L" {
      merged := make([]*AttrValue, 0, len(l.L)+len(r.L))
      merged = append(merged, l.L...)
      merged = append(merged, r.L...)
      return LVal(merged), nil
    }
    return nil, errValidation("cannot concatenate %s and %s", l.Type, r.Type)
  }
  if l.Type != "N" || r.Type != "N" {
    return nil, errValidation("operator %q requires numbers, found %s and %s", x.Op, l.Type, r.Type)
  }
  s, err := arith(l.N, r.N, x.Op)
  if err != nil {
    return nil, err
  }
  return NVal(s), nil
}

func evalValueFunc(c *evalCtx, x *FuncExpr) (*AttrValue, error) {
  switch x.Name {
  case "list_append":
    if len(x.Args) != 2 {
      return nil, errValidation("list_append expects 2 arguments")
    }
    l, err := evalValue(c, x.Args[0])
    if err != nil {
      return nil, err
    }
    r, err := evalValue(c, x.Args[1])
    if err != nil {
      return nil, err
    }
    if l.Type != "L" || r.Type != "L" {
      return nil, errValidation("list_append arguments must be lists")
    }
    merged := make([]*AttrValue, 0, len(l.L)+len(r.L))
    merged = append(merged, l.L...)
    merged = append(merged, r.L...)
    return LVal(merged), nil
  case "if_not_exists":
    if len(x.Args) != 2 {
      return nil, errValidation("if_not_exists expects 2 arguments")
    }
    if px, ok := x.Args[0].(*PathExpr); ok {
      v, found, err := resolvePath(c, px)
      if err != nil {
        return nil, err
      }
      if found {
        return v.Clone(), nil
      }
      return evalValue(c, x.Args[1])
    }
    return nil, errValidation("if_not_exists expects a path as its first argument")
  }
  return nil, errValidation("function %s cannot be used in a value context", x.Name)
}

// evalBool evaluates an expression as a boolean condition. Comparisons
// involving missing attributes are false; NOT inverts the resulting boolean.
func evalBool(c *evalCtx, e Expr) (bool, error) {
  switch x := e.(type) {
  case *BinExpr:
    switch x.Op {
    case "AND":
      l, err := evalBool(c, x.L)
      if err != nil {
        return false, err
      }
      if !l {
        return false, nil
      }
      return evalBool(c, x.R)
    case "OR":
      l, err := evalBool(c, x.L)
      if err != nil {
        return false, err
      }
      if l {
        return true, nil
      }
      return evalBool(c, x.R)
    case "=", "<>", "<", "<=", ">", ">=":
      return evalComparison(c, x)
    }
    return false, errValidation("operator %q is not a condition operator", x.Op)
  case *UnaryExpr:
    if x.Op == "NOT" {
      v, err := evalBool(c, x.X)
      if err != nil {
        return false, err
      }
      return !v, nil
    }
    return false, errValidation("expression is not a condition")
  case *BetweenExpr:
    xv, xok, err := gentleValue(c, x.X)
    if err != nil {
      return false, err
    }
    lov, look, err := gentleValue(c, x.Lo)
    if err != nil {
      return false, err
    }
    hiv, hiok, err := gentleValue(c, x.Hi)
    if err != nil {
      return false, err
    }
    res := false
    if xok && look && hiok {
      if c1, err1 := CompareAttr(xv, lov); err1 == nil {
        if c2, err2 := CompareAttr(xv, hiv); err2 == nil {
          res = c1 >= 0 && c2 <= 0
        }
      }
    }
    if x.Neg {
      res = !res
    }
    return res, nil
  case *InExpr:
    xv, xok, err := gentleValue(c, x.X)
    if err != nil {
      return false, err
    }
    res := false
    if xok {
      for _, m := range x.List {
        mv, mok, merr := gentleValue(c, m)
        if merr != nil {
          return false, merr
        }
        if mok && EqualAttr(xv, mv) {
          res = true
          break
        }
      }
    }
    if x.Neg {
      res = !res
    }
    return res, nil
  case *IsExpr:
    px, ok := x.X.(*PathExpr)
    if !ok {
      return false, errValidation("IS NULL / IS MISSING requires a path")
    }
    v, found, err := resolvePath(c, px)
    if err != nil {
      return false, err
    }
    var res bool
    if x.Kind == "NULL" {
      res = found && v != nil && v.Type == "NULL"
    } else {
      res = !found
    }
    if x.Neg {
      res = !res
    }
    return res, nil
  case *FuncExpr:
    return evalCondFunc(c, x)
  case *PathExpr:
    v, found, err := resolvePath(c, x)
    if err != nil {
      return false, err
    }
    if !found || v.Type != "BOOL" {
      return false, nil
    }
    return v.BOOL, nil
  case *LitExpr:
    if x.Missing {
      return false, nil
    }
    if x.Val != nil && x.Val.Type == "BOOL" {
      return x.Val.BOOL, nil
    }
    return false, errValidation("literal cannot be used as a condition")
  }
  return false, errValidation("expression is not a condition")
}

func evalComparison(c *evalCtx, x *BinExpr) (bool, error) {
  lv, lok, err := gentleValue(c, x.L)
  if err != nil {
    return false, err
  }
  rv, rok, err := gentleValue(c, x.R)
  if err != nil {
    return false, err
  }
  if !lok || !rok {
    return false, nil
  }
  switch x.Op {
  case "=":
    return EqualAttr(lv, rv), nil
  case "<>":
    return !EqualAttr(lv, rv), nil
  }
  cmp, cerr := CompareAttr(lv, rv)
  if cerr != nil {
    // Ordering comparisons across incomparable types simply fail.
    return false, nil
  }
  switch x.Op {
  case "<":
    return cmp < 0, nil
  case "<=":
    return cmp <= 0, nil
  case ">":
    return cmp > 0, nil
  case ">=":
    return cmp >= 0, nil
  }
  return false, errValidation("unknown comparison operator %q", x.Op)
}

func evalCondFunc(c *evalCtx, x *FuncExpr) (bool, error) {
  pathArg := func(i int) (*AttrValue, bool, error) {
    if i >= len(x.Args) {
      return nil, false, errValidation("%s: missing argument %d", strings.ToUpper(x.Name), i+1)
    }
    px, ok := x.Args[i].(*PathExpr)
    if !ok {
      return nil, false, errValidation("%s argument %d must be an attribute path", strings.ToUpper(x.Name), i+1)
    }
    return resolvePath(c, px)
  }
  switch x.Name {
  case "attribute_exists":
    if len(x.Args) != 1 {
      return false, errValidation("attribute_exists expects 1 argument")
    }
    _, found, err := pathArg(0)
    return found, err
  case "attribute_not_exists":
    if len(x.Args) != 1 {
      return false, errValidation("attribute_not_exists expects 1 argument")
    }
    _, found, err := pathArg(0)
    return !found, err
  case "attribute_type":
    if len(x.Args) != 2 {
      return false, errValidation("attribute_type expects 2 arguments")
    }
    v, found, err := pathArg(0)
    if err != nil || !found {
      return false, err
    }
    t, err := evalValue(c, x.Args[1])
    if err != nil {
      return false, err
    }
    if t.Type != "S" {
      return false, errValidation("attribute_type type argument must be a string")
    }
    return strings.EqualFold(v.Type, t.S), nil
  case "begins_with":
    if len(x.Args) != 2 {
      return false, errValidation("begins_with expects 2 arguments")
    }
    v, found, err := pathArg(0)
    if err != nil {
      return false, err
    }
    if !found {
      return false, nil
    }
    pv, err := evalValue(c, x.Args[1])
    if err != nil {
      return false, err
    }
    if v.Type == "S" && pv.Type == "S" {
      return strings.HasPrefix(v.S, pv.S), nil
    }
    if v.Type == "B" && pv.Type == "B" {
      return bytes.HasPrefix(v.B, pv.B), nil
    }
    return false, nil
  case "contains":
    if len(x.Args) != 2 {
      return false, errValidation("contains expects 2 arguments")
    }
    v, found, err := pathArg(0)
    if err != nil {
      return false, err
    }
    if !found {
      return false, nil
    }
    pv, err := evalValue(c, x.Args[1])
    if err != nil {
      return false, err
    }
    switch v.Type {
    case "S":
      if pv.Type != "S" {
        return false, errValidation("contains on a string attribute requires a string operand")
      }
      return strings.Contains(v.S, pv.S), nil
    case "SS":
      if pv.Type != "S" {
        return false, nil
      }
      return memberSet(v.SS, pv.S, false), nil
    case "NS":
      if pv.Type != "N" {
        return false, nil
      }
      return memberSet(v.NS, pv.N, true), nil
    case "BS":
      if pv.Type != "B" {
        return false, nil
      }
      for _, b := range v.BS {
        if bytes.Equal(b, pv.B) {
          return true, nil
        }
      }
      return false, nil
    case "L":
      return containsDeep(v.L, pv), nil
    }
    return false, errValidation("contains requires a string, set, or list attribute")
  }
  return false, errValidation("function %s cannot be used as a condition", x.Name)
}

func memberSet(xs []string, x string, numeric bool) bool {
  for _, y := range xs {
    if numeric {
      if c, err := CompareDecimal(x, y); err == nil && c == 0 {
        return true
      }
    } else if x == y {
      return true
    }
  }
  return false
}

func containsDeep(list []*AttrValue, v *AttrValue) bool {
  for _, e := range list {
    if EqualAttr(e, v) {
      return true
    }
  }
  return false
}
