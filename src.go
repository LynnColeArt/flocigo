// floci-go ticket-015 — UpdateItem over the DynamoDB JSON wire API.
//
// Implements UpdateItem with SET / REMOVE / ADD / DELETE actions (including the
// scoped operators list_append, if_not_exists and +/- arithmetic) plus an
// optional ConditionExpression (full condition-expression kernel: comparisons,
// BETWEEN, IN, size(), contains(), begins_with(), attribute_type(),
// attribute_exists/attribute_not_exists, NOT/AND/OR, nested document paths).
// Also provides the minimal table/item plumbing needed to exercise UpdateItem
// end-to-end (CreateTable, DescribeTable, DeleteTable, ListTables, PutItem,
// GetItem, DeleteItem, BatchWriteItem). Listens on http://:8000 and routes on
// the X-Amz-Target: DynamoDB_20120810.<Action> header (settable via $PORT).

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// API errors
// ---------------------------------------------------------------------------

type apiErr struct {
	Status int
	Type   string
	Msg    string
	Extra  map[string]any
}

func (e *apiErr) Error() string { return e.Type + ": " + e.Msg }

func verr(msg string) *apiErr {
	return &apiErr{Status: 400, Type: "com.amazon.coral.validate#ValidationException", Msg: msg}
}

func serErr() *apiErr {
	return &apiErr{Status: 400, Type: "com.amazon.coral.service#SerializationException"}
}

func condFailedErr(item map[string]*av) *apiErr {
	e := &apiErr{Status: 400, Type: "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException", Msg: "The conditional request failed"}
	if item != nil && len(item) > 0 {
		e.Extra = map[string]any{"Item": docToAny(item)}
	}
	return e
}

func tableNotFoundErr(name string) *apiErr {
	return &apiErr{Status: 400, Type: "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException", Msg: "Requested resource not found: Table: " + name + " not found"}
}

func tableInUseErr(name string) *apiErr {
	return &apiErr{Status: 400, Type: "com.amazonaws.dynamodb.v20120810#ResourceInUseException", Msg: "Table already exists: " + name}
}

func requiredNullErr(field string) *apiErr {
	return verr("1 validation error detected: Value null at '" + field + "' failed to satisfy constraint: Member must not be null")
}

func enumFailErr(field, val, set string) *apiErr {
	return verr("1 validation error detected: Value '" + val + "' at '" + field + "' failed to satisfy constraint: Member must satisfy enum value set: [" + set + "]")
}

// ---------------------------------------------------------------------------
// Attribute values
// ---------------------------------------------------------------------------

type av struct {
	T    string
	S    string
	N    string
	B    []byte
	Bool bool
	SS   []string
	NS   []string
	BS   [][]byte
	M    map[string]*av
	L    []*av
}

func avClone(x *av) *av {
	if x == nil {
		return nil
	}
	c := &av{T: x.T}
	switch x.T {
	case "S":
		c.S = x.S
	case "N":
		c.N = x.N
	case "B":
		c.B = append([]byte(nil), x.B...)
	case "BOOL":
		c.Bool = x.Bool
	case "SS":
		c.SS = append([]string(nil), x.SS...)
	case "NS":
		c.NS = append([]string(nil), x.NS...)
	case "BS":
		for _, b := range x.BS {
			c.BS = append(c.BS, append([]byte(nil), b...))
		}
	case "M":
		c.M = make(map[string]*av, len(x.M))
		for k, v := range x.M {
			c.M[k] = avClone(v)
		}
	case "L":
		if x.L != nil {
			c.L = make([]*av, 0, len(x.L))
			for _, v := range x.L {
				c.L = append(c.L, avClone(v))
			}
		}
	}
	return c
}

func docClone(d map[string]*av) map[string]*av {
	out := make(map[string]*av, len(d))
	for k, v := range d {
		out[k] = avClone(v)
	}
	return out
}

func avToAny(x *av) any {
	m := map[string]any{}
	switch x.T {
	case "S":
		m["S"] = x.S
	case "N":
		m["N"] = x.N
	case "B":
		if x.B == nil {
			m["B"] = []byte{}
		} else {
			m["B"] = x.B
		}
	case "BOOL":
		m["BOOL"] = x.Bool
	case "NULL":
		m["NULL"] = true
	case "SS":
		if x.SS == nil {
			m["SS"] = []string{}
		} else {
			m["SS"] = x.SS
		}
	case "NS":
		if x.NS == nil {
			m["NS"] = []string{}
		} else {
			m["NS"] = x.NS
		}
	case "BS":
		out := make([]any, 0, len(x.BS))
		for _, b := range x.BS {
			if b == nil {
				out = append(out, []byte{})
			} else {
				out = append(out, b)
			}
		}
		m["BS"] = out
	case "M":
		in := map[string]any{}
		for k, v := range x.M {
			in[k] = avToAny(v)
		}
		m["M"] = in
	case "L":
		out := make([]any, 0, len(x.L))
		for _, v := range x.L {
			out = append(out, avToAny(v))
		}
		m["L"] = out
	}
	return m
}

func docToAny(d map[string]*av) map[string]any {
	out := make(map[string]any, len(d))
	for k, v := range d {
		out[k] = avToAny(v)
	}
	return out
}

const badAVMsg = "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"

func parseAV(raw json.RawMessage) (*av, *apiErr) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || len(m) == 0 {
		return nil, verr(badAVMsg)
	}
	if len(m) > 1 {
		return nil, verr("Supplied AttributeValue has more than one datatypes set, should be exactly one")
	}
	for k, v := range m {
		switch k {
		case "S":
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, verr(badAVMsg)
			}
			return &av{T: "S", S: s}, nil
		case "N":
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, verr(badAVMsg)
			}
			if _, aerr := parseDecStr(s); aerr != nil {
				return nil, aerr
			}
			return &av{T: "N", N: s}, nil
		case "B":
			var b []byte
			if err := json.Unmarshal(v, &b); err != nil {
				return nil, verr(badAVMsg)
			}
			return &av{T: "B", B: b}, nil
		case "BOOL":
			var bb bool
			if err := json.Unmarshal(v, &bb); err != nil {
				return nil, verr(badAVMsg)
			}
			return &av{T: "BOOL", Bool: bb}, nil
		case "NULL":
			return &av{T: "NULL"}, nil
		case "SS":
			var list []string
			if err := json.Unmarshal(v, &list); err != nil {
				return nil, verr(badAVMsg)
			}
			return &av{T: "SS", SS: list}, nil
		case "NS":
			var list []string
			if err := json.Unmarshal(v, &list); err != nil {
				return nil, verr(badAVMsg)
			}
			for _, e := range list {
				if _, aerr := parseDecStr(e); aerr != nil {
					return nil, aerr
				}
			}
			return &av{T: "NS", NS: list}, nil
		case "BS":
			var list [][]byte
			if err := json.Unmarshal(v, &list); err != nil {
				return nil, verr(badAVMsg)
			}
			return &av{T: "BS", BS: list}, nil
		case "M":
			var mm map[string]json.RawMessage
			if err := json.Unmarshal(v, &mm); err != nil {
				return nil, verr(badAVMsg)
			}
			inner := make(map[string]*av, len(mm))
			for ik, iv := range mm {
				ia, aerr := parseAV(iv)
				if aerr != nil {
					return nil, aerr
				}
				inner[ik] = ia
			}
			return &av{T: "M", M: inner}, nil
		case "L":
			var ll []json.RawMessage
			if err := json.Unmarshal(v, &ll); err != nil {
				return nil, verr(badAVMsg)
			}
			elems := make([]*av, 0, len(ll))
			for _, iv := range ll {
				ia, aerr := parseAV(iv)
				if aerr != nil {
					return nil, aerr
				}
				elems = append(elems, ia)
			}
			return &av{T: "L", L: elems}, nil
		}
	}
	return nil, verr(badAVMsg)
}

// ---------------------------------------------------------------------------
// Exact-decimal arithmetic for the DynamoDB number type (38 significant digits)
// ---------------------------------------------------------------------------

type dec struct {
	neg bool
	dig string // normalized digit string, no leading/trailing zeros; "0" = zero
	exp int    // value = dig * 10^exp
}

func decZero() dec { return dec{dig: "0"} }
func (d dec) isZero() bool {	{ return d.dig == "0" } }

func precDigits(s string) int {
	t := strings.TrimLeft(s, "0")
	t = strings.TrimRight(t, "0")
	return len(t)
}

func parseDecStr(s string) (dec, *apiErr) {
	orig := s
	bad := verr("The parameter cannot be converted to a numeric value: " + orig)
	i, n := 0, len(s)
	neg := false
	if i < n && (s[i] == '-' || s[i] == '+') {
		neg = s[i] == '-'
		i++
	}
	start := i
	for i < n && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	ip := s[start:i]
	fp := ""
	if i < n && s[i] == '.' {
		i++
		fs := i
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		fp = s[fs:i]
	}
	exp := 0
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		esign := 1
		if i < n && (s[i] == '+' || s[i] == '-') {
			if s[i] == '-' {
				esign = -1
			}
			i++
		}
		es := i
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == es {
			return dec{}, bad
		}
		ev := 0
		for _, c := range s[es:i] {
			ev = ev*10 + int(c-'0')
			if ev > 100000 {
				ev = 100000
				break
			}
		}
		exp = esign * ev
	}
	if ip == "" && fp == "" {
		return dec{}, bad
	}
	if i != n {
		return dec{}, bad
	}
	dig := ip + fp
	exp -= len(fp)
	if precDigits(dig) > 38 {
		return dec{}, verr("Number precision. Attempting to store a number with more significant digits than supported.")
	}
	d := dec{neg: neg, dig: dig, exp: exp}
	d.norm()
	if aerr := checkRange(d); aerr != nil {
		return dec{}, aerr
	}
	return d, nil
}

func (d *dec) norm() {
	t := strings.TrimLeft(d.dig, "0")
	if t == "" {
		*d = decZero()
		return
	}
	tz := 0
	for strings.HasSuffix(t, "0") {
		t = t[:len(t)-1]
		tz++
	}
	d.dig = t
	d.exp += tz
}

func decFormat(d dec) string {
	if d.isZero() {
		return "0"
	}
	pre := ""
	if d.neg {
		pre = "-"
	}
	if d.exp >= 0 {
		return pre + d.dig + strings.Repeat("0", d.exp)
	}
	k := len(d.dig) + d.exp
	if k > 0 {
		return pre + d.dig[:k] + "." + d.dig[k:]
	}
	if k == 0 {
		return pre + "0." + d.dig
	}
	return pre + "0." + strings.Repeat("0", -k) + d.dig
}

func decCmp(a, b dec) int {
	az, bz := a.isZero(), b.isZero()
	if az && bz {
		return 0
	}
	if az {
		if b.neg {
			return 1
		}
		return -1
	}
	if bz {
		if a.neg {
			return -1
		}
		return 1
	}
	if a.neg != b.neg {
		if a.neg {
			return -1
		}
		return 1
	}
	r := 0
	ma := len(a.dig) + a.exp
	mb := len(b.dig) + b.exp
	if ma < mb {
		r = -1
	} else if ma > mb {
		r = 1
	}
	if r == 0 {
		x, y := a.dig, b.dig
		la, lb := len(x), len(y)
		if la < lb {
			x = x + strings.Repeat("0", lb-la)
		} else if lb < la {
			y = y + strings.Repeat("0", la-lb)
		}
		switch {
		case x < y:
			r = -1
		case x > y:
			r = 1
		}
	}
	if a.neg {
		r = -r
	}
	return r
}

func addDigits(a, b string) string {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	if len(a) < n {
		a = strings.Repeat("0", n-len(a)) + a
	}
	if len(b) < n {
		b = strings.Repeat("0", n-len(b)) + b
	}
	buf := make([]byte, n)
	carry := 0
	for i := n - 1; i >= 0; i-- {
		v := int(a[i]-'0') + int(b[i]-'0') + carry
		if v >= 10 {
			v -= 10
			carry = 1
		} else {
			carry = 0
		}
		buf[i] = byte('0' + v)
	}
	s := string(buf)
	if carry > 0 {
		s = "1" + s
	}
	return s
}

func subDigits(a, b string) string {
	n := len(a)
	if len(b) < n {
		b = strings.Repeat("0", n-len(b)) + b
	}
	buf := make([]byte, n)
	carry := 0
	for i := n - 1; i >= 0; i-- {
		v := int(a[i]-'0') - int(b[i]-'0') - carry
		if v < 0 {
			v += 10
			carry = 1
		} else {
			carry = 0
		}
		buf[i] = byte('0' + v)
	}
	return string(buf)
}

func cmpDigitsInt(a, b string) int {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	if len(a) < n {
		a = strings.Repeat("0", n-len(a)) + a
	}
	if len(b) < n {
		b = strings.Repeat("0", n-len(b)) + b
	}
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func roundDec(d dec) dec {
	if len(d.dig) <= 38 {
		return d
	}
	kept := d.dig[:38]
	rest := d.dig[38:]
	up := false
	switch {
	case rest[0] > '5':
		up = true
	case rest[0] == '5':
		if len(rest) > 1 {
			up = true
		} else if (kept[len(kept)-1]-'0')%2 == 1 {
			up = true
		}
	}
	if up {
		out := []byte(kept)
		carry := 1
		for i := len(out) - 1; i >= 0 && carry > 0; i-- {
			v := int(out[i]-'0') + carry
			if v == 10 {
				out[i] = '0'
			} else {
				out[i] = byte('0' + v)
				carry = 0
			}
		}
		if carry > 0 {
			d.exp += 38
			d.dig = "1"
		} else {
			d.dig = string(out)
		}
	} else {
		d.dig = kept
	}
	d.norm()
	return d
}

func checkRange(d dec) *apiErr {
	if d.isZero() {
		return nil
	}
	mag := d.exp + len(d.dig) - 1
	if mag >= 126 {
		return verr("Number overflow. Attempting to store a number with magnitude larger than supported range.")
	}
	if mag < -130 {
		return verr("Number underflow. Attempting to store a number with magnitude smaller than supported range.")
	}
	return nil
}

func decAdd(a, b dec) (dec, *apiErr) {
	if a.isZero() {
		if aerr := checkRange(b); aerr != nil {
			return dec{}, aerr
		}
		return b, nil
	}
	if b.isZero() {
		if aerr := checkRange(a); aerr != nil {
			return dec{}, aerr
		}
		return a, nil
	}
	e := a.exp
	if b.exp < e {
		e = b.exp
	}
	sa := a.dig + strings.Repeat("0", a.exp-e)
	sb := b.dig + strings.Repeat("0", b.exp-e)
	var digits string
	var neg bool
	if a.neg == b.neg {
		digits = addDigits(sa, sb)
		neg = a.neg
	} else {
		c := cmpDigitsInt(sa, sb)
		if c == 0 {
			return decZero(), nil
		}
		if c > 0 {
			digits = subDigits(sa, sb)
			neg = a.neg
		} else {
			digits = subDigits(sb, sa)
			neg = b.neg
		}
	}
	d := dec{neg: neg, dig: digits, exp: e}
	d.norm()
	d = roundDec(d)
	if aerr := checkRange(d); aerr != nil {
		return dec{}, aerr
	}
	return d, nil
}

// ---------------------------------------------------------------------------
// Expression engine kernel: lexer, document paths, conditions, updates
// ---------------------------------------------------------------------------

const (
	kUpdate    = "update"
	kCondition = "condition"
	kProj      = "projection"
)

const (
	msgNotExists   = "The provided expression refers to an attribute that does not exist in the item"
	msgInvalidPath = "The document path provided in the update expression is invalid for update"
)

var reservedWords = map[string]bool{
	"ADD": true, "ALL": true, "AND": true, "ANY": true, "ARRAY": true, "AS": true, "ASC": true,
	"ATTRIBUTE": true, "ATTRIBUTES": true, "BETWEEN": true, "BOOL": true, "BOOLEAN": true, "BY": true,
	"CASE": true, "CAST": true, "CONDITION": true, "CONTAINS": true, "COUNT": true, "DECIMAL": true,
	"DEFAULT": true, "DELETE": true, "DESC": true, "DISTINCT": true, "ELSE": true, "EMPTY": true,
	"END": true, "EXISTS": true, "FALSE": true, "FROM": true, "GET": true, "GLOBAL": true,
	"HASH": true, "HOUR": true, "IF": true, "IN": true, "INDEX": true, "INDEXES": true,
	"INSERT": true, "INTO": true, "IS": true, "ITEM": true, "ITEMS": true, "KEY": true,
	"LAST": true, "LEADING": true, "LEFT": true, "LEVEL": true, "LIKE": true, "LIST": true,
	"LOCAL": true, "LOW": true, "MAP": true, "MINUTE": true, "MONTH": true, "NAME": true,
	"NAMES": true, "NOT": true, "NULL": true, "NUMBER": true, "OF": true, "ON": true, "OR": true,
	"ORDER": true, "PARTITION": true, "PATH": true, "PRIMARY": true, "PRIVILEGES": true,
	"REMOVE": true, "RETURNING": true, "RIGHT": true, "ROUND": true, "ROW": true, "ROWS": true,
	"SCHEMA": true, "SECOND": true, "SELECT": true, "SET": true, "SIZE": true, "SOME": true,
	"SUBSTRING": true, "SUM": true, "TABLE": true, "TABLES": true, "TAILING": true, "THEN": true,
	"TIMESTAMP": true, "TO": true, "TOP": true, "TRAILING": true, "TRUE": true, "TRUNCATE": true,
	"UNION": true, "UNKNOWN": true, "UPDATE": true, "USING": true, "VALIDATE": true, "VALUE": true,
	"VALUES": true, "VIEW": true, "WHERE": true, "WITH": true, "YEAR": true,
}

func isReserved(w string) bool { return reservedWords[strings.ToUpper(w)] }

type token struct{ K, V string }

type perr struct {
	kind     string
	msg      string
	noPrefix bool
}

func (e *perr) api() *apiErr {
	if e.noPrefix {
		return verr(e.msg)
	}
	prefix := "Invalid ConditionExpression: "
	switch e.kind {
	case kUpdate:
		prefix = "Invalid UpdateExpression: "
	case kProj:
		prefix = "Invalid ProjectionExpression: "
	}
	return verr(prefix + e.msg)
}

func exprPhrase(kind string) string {
	switch kind {
	case kUpdate:
		return "update expression"
	case kCondition:
		return "condition expression"
	case kProj:
		return "projection expression"
	}
	return "expression"
}

func syntaxErr(kind, tok string) *perr {
	if tok == "" {
		return &perr{kind: kind, msg: `Syntax error; token: <end>`}
	}
	return &perr{kind: kind, msg: `Syntax error; token: "` + tok + `"`}
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func lex(src, kind string) ([]token, *perr) {
	var out []token
	i, n := 0, len(src)
	for i < n {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#':
			j := i + 1
			for j < n && isIdentByte(src[j]) {
				j++
			}
			if j == i+1 {
				return nil, syntaxErr(kind, "#")
			}
			out = append(out, token{"ref", src[i+1 : j]})
			i = j
		case c == ':':
			j := i + 1
			for j < n && isIdentByte(src[j]) {
				j++
			}
			if j == i+1 {
				return nil, syntaxErr(kind, ":")
			}
			out = append(out, token{"val", src[i+1 : j]})
			i = j
		case c == '\'':
			j := i + 1
			var sb strings.Builder
			closed := false
			for j < n {
				if src[j] == '\'' {
					if j+1 < n && src[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				sb.WriteByte(src[j])
				j++
			}
			if !closed {
				return nil, syntaxErr(kind, "'")
			}
			out = append(out, token{"str", sb.String()})
			i = j
		case isIdentStart(c):
			j := i
			for j < n && isIdentByte(src[j]) {
				j++
			}
			out = append(out, token{"id", src[i:j]})
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < n && src[j] >= '0' && src[j] <= '9' {
				j++
			}
			out = append(out, token{"num", src[i:j]})
			i = j
		case c == '<' || c == '>':
			if i+1 < n && (src[i+1] == '=' || (c == '<' && src[i+1] == '>')) {
				out = append(out, token{"sym", src[i : i+2]})
				i += 2
			} else {
				out = append(out, token{"sym", string(c)})
				i++
			}
		case strings.IndexByte("()[],.=+-", c) >= 0:
			out = append(out, token{"sym", string(c)})
			i++
		default:
			return nil, syntaxErr(kind, string(c))
		}
	}
	return out, nil
}

type pathElem struct {
	nameStr string // literal attribute name, or "#x" placeholder
	idx     []int
}

type docPath []pathElem

type parser struct {
	toks  []token
	i     int
	kind  string
	usedN map[string]bool
	usedV map[string]bool
}

func (p *parser) peekTok() (token, bool) {
	if p.i < len(p.toks) {
		return p.toks[p.i], true
	}
	return token{}, false
}

func (p *parser) peekAt(n int) (token, bool) {
	k := p.i + n
	if k < len(p.toks) {
		return p.toks[k], true
	}
	return token{}, false
}

func (p *parser) nextTok() (token, bool) {
	t, ok := p.peekTok()
	if ok {
		p.i++
	}
	return t, ok
}

func (p *parser) hasSym(v string) bool {
	t, ok := p.peekTok()
	return ok && t.K == "sym" && t.V == v
}

func (p *parser) isKeyword(w string) bool {
	t, ok := p.peekTok()
	return ok && t.K == "id" && strings.ToUpper(t.V) == w
}

func (p *parser) expectSym(v string) *perr {
	if p.hasSym(v) {
		p.i++
		return nil
	}
	if t, ok := p.peekTok(); ok {
		return p.syn(t.V)
	}
	return p.syn("")
}

func (p *parser) syn(tok string) *perr { return syntaxErr(p.kind, tok) }

func (p *parser) parseDocPath() (docPath, *perr) {
	var path docPath
	for {
		t, ok := p.nextTok()
		if !ok {
			return nil, p.syn("")
		}
		var el pathElem
		switch t.K {
		case "ref":
			p.usedN["#"+t.V] = true
			el.nameStr = "#" + t.V
		case "id":
			if isReserved(t.V) {
				return nil, &perr{kind: p.kind, msg: "Attribute name is a reserved keyword; reserved keyword: " + t.V}
			}
			el.nameStr = t.V
		default:
			return nil, p.syn(t.V)
		}
		for {
			if p.hasSym("[") {
				p.i++
				t2, ok2 := p.nextTok()
				if !ok2 {
					return nil, p.syn("")
				}
				if t2.K != "num" {
					return nil, p.syn(t2.V)
				}
				iv := 0
				for k := 0; k < len(t2.V); k++ {
					iv = iv*10 + int(t2.V[k]-'0')
				}
				t3, ok3 := p.nextTok()
				if !ok3 || t3.K != "sym" || t3.V != "]" {
					if !ok3 {
						return nil, p.syn("")
					}
					return nil, p.syn(t3.V)
				}
				el.idx = append(el.idx, iv)
				continue
			}
			break
		}
		path = append(path, el)
		if p.hasSym(".") {
			p.i++
			continue
		}
		break
	}
	return path, nil
}

func resolvePathNames(p docPath, names map[string]string, phrase string) *perr {
	for i := range p {
		if strings.HasPrefix(p[i].nameStr, "#") {
			v, ok := names[p[i].nameStr]
			if !ok {
				return &perr{noPrefix: true, msg: "An expression attribute name used in the " + phrase + " is not defined; undefined attribute name: " + p[i].nameStr}
			}
			p[i].nameStr = v
		}
	}
	return nil
}

// ----- document-path walking / mutation -------------------------------------

func walkDoc(doc map[string]*av, path docPath, strict bool) (*av, bool, *apiErr) {
	cur := &av{T: "M", M: doc}
	for _, el := range path {
		if cur.T != "M" {
			return nil, false, verr(msgInvalidPath)
		}
		v, ok := cur.M[el.nameStr]
		if !ok {
			if strict {
				return nil, false, verr(msgNotExists)
			}
			return nil, false, nil
		}
		cur = v
		for _, ix := range el.idx {
			if cur.T != "L" {
				return nil, false, verr(msgInvalidPath)
			}
			if ix >= len(cur.L) {
				if strict {
					return nil, false, verr(msgNotExists)
				}
				return nil, false, nil
			}
			cur = cur.L[ix]
		}
	}
	return cur, true, nil
}

type step struct {
	isIdx bool
	name  string
	idx   int
}

func flattenPath(p docPath) []step {
	out := make([]step, 0, len(p)*2)
	for _, el := range p {
		out = append(out, step{name: el.nameStr})
		for _, ix := range el.idx {
			out = append(out, step{isIdx: true, idx: ix})
		}
	}
	return out
}

func setLeaf(doc map[string]*av, p docPath, val *av) *apiErr {
	steps := flattenPath(p)
	cur := &av{T: "M", M: doc}
	for k := 0; k < len(steps)-1; k++ {
		st := steps[k]
		if st.isIdx {
			if cur.T != "L" || st.idx >= len(cur.L) {
				return verr(msgInvalidPath)
			}
			cur = cur.L[st.idx]
		} else {
			if cur.T != "M" {
				return verr(msgInvalidPath)
			}
			nv, ok := cur.M[st.name]
			if !ok {
				nv = &av{T: "M", M: map[string]*av{}}
				cur.M[st.name] = nv
			}
			cur = nv
		}
	}
	leaf := steps[len(steps)-1]
	if leaf.isIdx {
		if cur.T != "L" {
			return verr(msgInvalidPath)
		}
		if leaf.idx == len(cur.L) {
			cur.L = append(cur.L, val)
			return nil
		}
		if leaf.idx < len(cur.L) {
			cur.L[leaf.idx] = val
			return nil
		}
		return verr(msgInvalidPath)
	}
	if cur.T != "M" {
		return verr(msgInvalidPath)
	}
	cur.M[leaf.name] = val
	return nil
}

func removeLeaf(doc map[string]*av, p docPath) *apiErr {
	steps := flattenPath(p)
	cur := &av{T: "M", M: doc}
	for k := 0; k < len(steps)-1; k++ {
		st := steps[k]
		if st.isIdx {
			if cur.T != "L" {
				return verr(msgInvalidPath)
			}
			if st.idx >= len(cur.L) {
				return nil
			}
			cur = cur.L[st.idx]
		} else {
			if cur.T != "M" {
				return verr(msgInvalidPath)
			}
			nv, ok := cur.M[st.name]
			if !ok {
				return nil
			}
			cur = nv
		}
	}
	leaf := steps[len(steps)-1]
	if leaf.isIdx {
		if cur.T != "L" {
			return verr(msgInvalidPath)
		}
		if leaf.idx < len(cur.L) {
			cur.L = append(cur.L[:leaf.idx], cur.L[leaf.idx+1:]...)
		}
		return nil
	}
	if cur.T != "M" {
		return verr(msgInvalidPath)
	}
	delete(cur.M, leaf.name)
	return nil
}

func addLeaf(doc map[string]*av, p docPath, val *av) *apiErr {
	if val.T != "N" && val.T != "SS" && val.T != "NS" && val.T != "BS" {
		return verr("Incorrect operand type for operator or function; operator or function: ADD, operand type: " + val.T)
	}
	steps := flattenPath(p)
	cur := &av{T: "M", M: doc}
	for k := 0; k < len(steps)-1; k++ {
		st := steps[k]
		if st.isIdx {
			if cur.T != "L" || st.idx >= len(cur.L) {
				return verr(msgInvalidPath)
			}
			cur = cur.L[st.idx]
		} else {
			if cur.T != "M" {
				return verr(msgInvalidPath)
			}
			nv, ok := cur.M[st.name]
			if !ok {
				nv = &av{T: "M", M: map[string]*av{}}
				cur.M[st.name] = nv
			}
			cur = nv
		}
	}
	leaf := steps[len(steps)-1]
	if leaf.isIdx {
		return verr(msgInvalidPath)
	}
	if cur.T != "M" {
		return verr(msgInvalidPath)
	}
	existing := cur.M[leaf.name]
	if existing == nil {
		cur.M[leaf.name] = val
		return nil
	}
	if existing.T == "N" && val.T == "N" {
		da, e1 := parseDecStr(existing.N)
		if e1 != nil {
			return e1
		}
		db, e2 := parseDecStr(val.N)
		if e2 != nil {
			return e2
		}
		sum, aerr := decAdd(da, db)
		if aerr != nil {
			return aerr
		}
		cur.M[leaf.name] = &av{T: "N", N: decFormat(sum)}
		return nil
	}
	if existing.T == val.T && (existing.T == "SS" || existing.T == "NS" || existing.T == "BS") {
		cur.M[leaf.name] = setUnion(existing, val)
		return nil
	}
	return verr("Incorrect operand type for operator or function; operator or function: ADD, operand type: " + existing.T)
}

func deleteLeaf(doc map[string]*av, p docPath, val *av) *apiErr {
	if val.T != "SS" && val.T != "NS" && val.T != "BS" {
		return verr("Incorrect operand type for operator or function; operator or function: DELETE, operand type: " + val.T)
	}
	steps := flattenPath(p)
	cur := &av{T: "M", M: doc}
	for k := 0; k < len(steps)-1; k++ {
		st := steps[k]
		if st.isIdx {
			if cur.T != "L" {
				return verr(msgInvalidPath)
			}
			if st.idx >= len(cur.L) {
				return nil
			}
			cur = cur.L[st.idx]
		} else {
			if cur.T != "M" {
				return verr(msgInvalidPath)
			}
			nv, ok := cur.M[st.name]
			if !ok {
				return nil
			}
			cur = nv
		}
	}
	leaf := steps[len(steps)-1]
	if leaf.isIdx {
		return verr(msgInvalidPath)
	}
	if cur.T != "M" {
		return verr(msgInvalidPath)
	}
	existing := cur.M[leaf.name]
	if existing == nil {
		return nil
	}
	if existing.T == val.T && (existing.T == "SS" || existing.T == "NS" || existing.T == "BS") {
		rem := setSubtract(existing, val)
		if setLen(rem) == 0 {
			delete(cur.M, leaf.name)
		} else {
			cur.M[leaf.name] = rem
		}
		return nil
	}
	return verr("Incorrect operand type for operator or function; operator or function: DELETE, operand type: " + existing.T)
}

func setLen(a *av) int {
	switch a.T {
	case "SS":
		return len(a.SS)
	case "NS":
		return len(a.NS)
	case "BS":
		return len(a.BS)
	}
	return 0
}

func setUnion(e, a *av) *av {
	out := avClone(e)
	switch e.T {
	case "SS":
		seen := map[string]bool{}
		for _, s := range out.SS {
			seen[s] = true
		}
		for _, s := range a.SS {
			if !seen[s] {
				seen[s] = true
				out.SS = append(out.SS, s)
			}
		}
	case "NS":
		seen := map[string]bool{}
		for _, s := range out.NS {
			if d, err := parseDecStr(s); err == nil {
				seen[decFormat(d)] = true
			} else {
				seen[s] = true
			}
		}
		for _, s := range a.NS {
			k := s
			if d, err := parseDecStr(s); err == nil {
				k = decFormat(d)
			}
			if !seen[k] {
				seen[k] = true
				out.NS = append(out.NS, s)
			}
		}
	case "BS":
		seen := map[string]bool{}
		for _, b := range out.BS {
			seen[string(b)] = true
		}
		for _, b := range a.BS {
			k := string(b)
			if !seen[k] {
				seen[k] = true
				out.BS = append(out.BS, append([]byte(nil), b...))
			}
		}
	}
	return out
}

func setSubtract(e, d *av) *av {
	out := avClone(e)
	switch e.T {
	case "SS":
		rm := map[string]bool{}
		for _, s := range d.SS {
			rm[s] = true
		}
		var keep []string
		for _, s := range out.SS {
			if !rm[s] {
				keep = append(keep, s)
			}
		}
		out.SS = keep
	case "NS":
		rm := map[string]bool{}
		for _, s := range d.NS {
			if dd, err := parseDecStr(s); err == nil {
				rm[decFormat(dd)] = true
			} else {
				rm[s] = true
			}
		}
		var keep []string
		for _, s := range out.NS {
			k := s
			if dd, err := parseDecStr(s); err == nil {
				k = decFormat(dd)
			}
			if !rm[k] {
				keep = append(keep, s)
			}
		}
		out.NS = keep
	case "BS":
		rm := map[string]bool{}
		for _, b := range d.BS {
			rm[string(b)] = true
		}
		var keep [][]byte
		for _, b := range out.BS {
			if !rm[string(b)] {
				keep = append(keep, b)
			}
		}
		out.BS = keep
	}
	return out
}

// ----- equality -------------------------------------------------------------

func avEqual(a, b *av) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.T != b.T {
		return false
	}
	switch a.T {
	case "S":
		return a.S == b.S
	case "N":
		da, e1 := parseDecStr(a.N)
		db, e2 := parseDecStr(b.N)
		if e1 != nil || e2 != nil {
			return a.N == b.N
		}
		return decCmp(da, db) == 0
	case "B":
		return bytes.Equal(a.B, b.B)
	case "BOOL":
		return a.Bool == b.Bool
	case "NULL":
		return true
	case "SS":
		return ssEqual(a.SS, b.SS)
	case "NS":
		return nsEqual(a.NS, b.NS)
	case "BS":
		return bsEqual(a.BS, b.BS)
	case "M":
		if len(a.M) != len(b.M) {
			return false
		}
		for k, v := range a.M {
			w, ok := b.M[k]
			if !ok || !avEqual(v, w) {
				return false
			}
		}
		return true
	case "L":
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !avEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func ssEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	m := make(map[string]bool, len(y))
	for _, v := range y {
		m[v] = true
	}
	for _, v := range x {
		if !m[v] {
			return false
		}
	}
	return true
}

func nsEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	m := make(map[string]bool, len(y))
	for _, v := range y {
		if d, err := parseDecStr(v); err == nil {
			m[decFormat(d)] = true
		} else {
			m[v] = true
		}
	}
	for _, v := range x {
		k := v
		if d, err := parseDecStr(v); err == nil {
			k = decFormat(d)
		}
		if !m[k] {
			return false
		}
	}
	return true
}

func bsEqual(x, y [][]byte) bool {
	if len(x) != len(y) {
		return false
	}
	m := make(map[string]bool, len(y))
	for _, v := range y {
		m[string(v)] = true
	}
	for _, v := range x {
		if !m[string(v)] {
			return false
		}
	}
	return true
}

// ----- update expressions ---------------------------------------------------

type operandArg struct {
	kind string // "path" | "val" | "lit" | "fn"
	path docPath
	tok  string // ":x" for val, literal string for lit
	fn   string
	arg  []*operandArg
}

type updOperand struct {
	first  *operandArg
	op     string // "", "+", "-"
	second *operandArg
}

type setAct struct {
	path docPath
	rhs  updOperand
}

type addAct struct {
	path docPath
	val  string
}

type updateExpr struct {
	sets    []setAct
	removes []docPath
	adds    []addAct
	deletes []addAct
	usedN   map[string]bool
	usedV   map[string]bool
	hasAny  bool
}

func isUpdateSection(s string) bool {
	switch s {
	case "SET", "REMOVE", "ADD", "DELETE":
		return true
	}
	return false
}

func parseUpdateExpr(src string) (*updateExpr, *perr) {
	toks, e := lex(src, kUpdate)
	if e != nil {
		return nil, e
	}
	p := &parser{toks: toks, kind: kUpdate, usedN: map[string]bool{}, usedV: map[string]bool{}}
	u := &updateExpr{usedN: p.usedN, usedV: p.usedV}
	for {
		t, ok := p.peekTok()
		if !ok {
			break
		}
		if t.K == "id" {
			sec := strings.ToUpper(t.V)
			if isUpdateSection(sec) {
				p.i++
				if e := p.actionsFor(u, sec); e != nil {
					return nil, e
				}
				for p.hasSym(",") {
					nt, ok2 := p.peekAt(1)
					if ok2 && nt.K == "id" && isUpdateSection(strings.ToUpper(nt.V)) {
						break
					}
					p.i++
					if e := p.actionsFor(u, sec); e != nil {
						return nil, e
					}
				}
				continue
			}
		}
		return nil, p.syn(t.V)
	}
	return u, nil
}

func (p *parser) actionsFor(u *updateExpr, section string) *perr {
	switch section {
	case "SET":
		path, e := p.parseDocPath()
		if e != nil {
			return e
		}
		t, ok := p.nextTok()
		if !ok {
			return p.syn("")
		}
		if t.K != "sym" || t.V != "=" {
			return p.syn(t.V)
		}
		rhs, e := p.parseUpdOperand()
		if e != nil {
			return e
		}
		u.sets = append(u.sets, setAct{path: path, rhs: rhs})
		u.hasAny = true
	case "REMOVE":
		path, e := p.parseDocPath()
		if e != nil {
			return e
		}
		u.removes = append(u.removes, path)
		u.hasAny = true
	case "ADD":
		path, vtok, e := p.pathPlusVal()
		if e != nil {
			return e
		}
		u.adds = append(u.adds, addAct{path: path, val: vtok})
		u.hasAny = true
	case "DELETE":
		path, vtok, e := p.pathPlusVal()
		if e != nil {
			return e
		}
		u.deletes = append(u.deletes, addAct{path: path, val: vtok})
		u.hasAny = true
	}
	return nil
}

func (p *parser) pathPlusVal() (docPath, string, *perr) {
	path, e := p.parseDocPath()
	if e != nil {
		return nil, "", e
	}
	t, ok := p.nextTok()
	if !ok {
		return nil, "", p.syn("")
	}
	if t.K != "val" {
		return nil, "", p.syn(t.V)
	}
	p.usedV[":"+t.V] = true
	return path, ":" + t.V, nil
}

func (p *parser) parseUpdOperand() (updOperand, *perr) {
	f, e := p.parseUpdTerm()
	if e != nil {
		return updOperand{}, e
	}
	op := ""
	var snd *operandArg
	if p.hasSym("+") || p.hasSym("-") {
		t, _ := p.nextTok()
		op = t.V
		snd, e = p.parseUpdTerm()
		if e != nil {
			return updOperand{}, e
		}
	}
	return updOperand{first: f, op: op, second: snd}, nil
}

func (p *parser) parseUpdTerm() (*operandArg, *perr) {
	t, ok := p.nextTok()
	if !ok {
		return nil, p.syn("")
	}
	switch t.K {
	case "val":
		p.usedV[":"+t.V] = true
		return &operandArg{kind: "val", tok: ":" + t.V}, nil
	case "ref", "id":
		if t.K == "id" {
			low := strings.ToLower(t.V)
			if low == "list_append" || low == "if_not_exists" {
				nt, ok2 := p.peekTok()
				if ok2 && nt.K == "sym" && nt.V == "(" {
					p.i++
					a1, e := p.parseUpdTerm()
					if e != nil {
						return nil, e
					}
					if e := p.expectSym(","); e != nil {
						return nil, e
					}
					a2, e := p.parseUpdTerm()
					if e != nil {
						return nil, e
					}
					if e := p.expectSym(")"); e != nil {
						return nil, e
					}
					return &operandArg{kind: "fn", fn: low, arg: []*operandArg{a1, a2}}, nil
				}
			}
		}
		p.i--
		path, e := p.parseDocPath()
		if e != nil {
			return nil, e
		}
		return &operandArg{kind: "path", path: path}, nil
	case "str":
		return nil, p.syn("'" + t.V + "'")
	}
	return nil, p.syn(t.V)
}

func uexActionPaths(u *updateExpr) []docPath {
	var out []docPath
	for i := range u.sets {
		out = append(out, u.sets[i].path)
	}
	out = append(out, u.removes...)
	for i := range u.adds {
		out = append(out, u.adds[i].path)
	}
	for i := range u.deletes {
		out = append(out, u.deletes[i].path)
	}
	return out
}

func walkUpdOperand(o *operandArg, out *[]docPath) {
	if o == nil {
		return
	}
	if o.kind == "path" {
		*out = append(*out, o.path)
	}
	for _, x := range o.arg {
		walkUpdOperand(x, out)
	}
}

func uexAllPaths(u *updateExpr) []docPath {
	var out []docPath
	for i := range u.sets {
		out = append(out, u.sets[i].path)
		walkUpdOperand(u.sets[i].rhs.first, &out)
		walkUpdOperand(u.sets[i].rhs.second, &out)
	}
	out = append(out, u.removes...)
	for i := range u.adds {
		out = append(out, u.adds[i].path)
	}
	for i := range u.deletes {
		out = append(out, u.deletes[i].path)
	}
	return out
}

func docPathOverlaps(a, b docPath) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i].nameStr != b[i].nameStr {
			return false
		}
		m := len(a[i].idx)
		if len(b[i].idx) < m {
			m = len(b[i].idx)
		}
		for k := 0; k < m; k++ {
			if a[i].idx[k] != b[i].idx[k] {
				return false
			}
		}
		if len(a[i].idx) != len(b[i].idx) {
			return true
		}
	}
	return true
}

func resolveUpdOperand(o updOperand, orig map[string]*av, vals map[string]*av) (*av, *apiErr) {
	a, aerr := resolveUpdTerm(o.first, orig, vals)
	if aerr != nil {
		return nil, aerr
	}
	if o.op == "" {
		return a, nil
	}
	b, aerr := resolveUpdTerm(o.second, orig, vals)
	if aerr != nil {
		return nil, aerr
	}
	opName := "ADD"
	if o.op == "-" {
		opName = "SUB"
	}
	if a.T != "N" {
		return nil, verr("Incorrect operand type for operator or function; operator or function: " + opName + ", operand type: " + a.T)
	}
	if b.T != "N" {
		return nil, verr("Incorrect operand type for operator or function; operator or function: " + opName + ", operand type: " + b.T)
	}
	da, e1 := parseDecStr(a.N)
	if e1 != nil {
		return nil, e1
	}
	db, e2 := parseDecStr(b.N)
	if e2 != nil {
		return nil, e2
	}
	if o.op == "-" {
		db.neg = !db.neg
	}
	sum, aerr := decAdd(da, db)
	if aerr != nil {
		return nil, aerr
	}
	return &av{T: "N", N: decFormat(sum)}, nil
}

func resolveUpdTerm(o *operandArg, orig map[string]*av, vals map[string]*av) (*av, *apiErr) {
	switch o.kind {
	case "val":
		return avClone(vals[o.tok]), nil
	case "path":
		v, aerr := walkDoc(orig, o.path, true)
		if aerr != nil {
			return nil, aerr
		}
		return avClone(v), nil
	case "fn":
		switch o.fn {
		case "list_append":
			x, aerr := resolveUpdTerm(o.arg[0], orig, vals)
			if aerr != nil {
				return nil, aerr
			}
			y, aerr := resolveUpdTerm(o.arg[1], orig, vals)
			if aerr != nil {
				return nil, aerr
			}
			if x.T != "L" {
				return nil, verr("Incorrect operand type for operator or function; operator or function: LIST_APPEND, operand type: " + x.T)
			}
			if y.T != "L" {
				return nil, verr("Incorrect operand type for operator or function; operator or function: LIST_APPEND, operand type: " + y.T)
			}
			outL := append(avClone(x).L, avClone(y).L...)
			return &av{T: "L", L: outL}, nil
		case "if_not_exists":
			if o.arg[0].kind != "path" {
				tname := strings.ToUpper(o.arg[0].fn)
				if o.arg[0].kind == "val" {
					tname = vals[o.arg[0].tok].T
				} else if o.arg[0].kind == "lit" {
					tname = "S"
				}
				return nil, verr("Incorrect operand type for operator or function; operator or function: IF_NOT_EXISTS, operand type: " + tname)
			}
			v, aerr := walkDoc(orig, o.arg[0].path, true)
			if aerr != nil {
				if aerr.Msg == msgNotExists {
					return resolveUpdTerm(o.arg[1], orig, vals)
				}
				return nil, aerr
			}
			return avClone(v), nil
		}
	}
	return nil, verr("Unexpected expression operand")
}

// ----- condition expressions ------------------------------------------------

type cnode struct {
	kind string // "logic" | "cmp" | "between" | "in" | "fn"
	op   string
	lf   *cnode
	rt   *cnode
	lo   *operandArg
	ro   *operandArg
	midA *operandArg
	midB *operandArg
	list []*operandArg
	fn   string
	farg []*operandArg
}

type condExpr struct {
	root  *cnode
	usedN map[string]bool
	usedV map[string]bool
}

func parseCondExpr(src string) (*condExpr, *perr) {
	toks, e := lex(src, kCondition)
	if e != nil {
		return nil, e
	}
	p := &parser{toks: toks, kind: kCondition, usedN: map[string]bool{}, usedV: map[string]bool{}}
	if len(toks) == 0 {
		return nil, p.syn("")
	}
	root, e := p.parseOr()
	if e != nil {
		return nil, e
	}
	if t, ok := p.peekTok(); ok {
		return nil, p.syn(t.V)
	}
	return &condExpr{root: root, usedN: p.usedN, usedV: p.usedV}, nil
}

func (p *parser) parseOr() (*cnode, *perr) {
	l, e := p.parseAnd()
	if e != nil {
		return nil, e
	}
	for p.isKeyword("OR") {
		p.i++
		r, e2 := p.parseAnd()
		if e2 != nil {
			return nil, e2
		}
		l = &cnode{kind: "logic", op: "OR", lf: l, rt: r}
	}
	return l, nil
}

func (p *parser) parseAnd() (*cnode, *perr) {
	l, e := p.parseNot()
	if e != nil {
		return nil, e
	}
	for p.isKeyword("AND") {
		p.i++
		r, e2 := p.parseNot()
		if e2 != nil {
			return nil, e2
		}
		l = &cnode{kind: "logic", op: "AND", lf: l, rt: r}
	}
	return l, nil
}

func (p *parser) parseNot() (*cnode, *perr) {
	if p.isKeyword("NOT") {
		p.i++
		inner, e := p.parseNot()
		if e != nil {
			return nil, e
		}
		return &cnode{kind: "logic", op: "NOT", lf: inner}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (*cnode, *perr) {
	if p.hasSym("(") {
		p.i++
		inner, e := p.parseOr()
		if e != nil {
			return nil, e
		}
		if e := p.expectSym(")"); e != nil {
			return nil, e
		}
		return inner, nil
	}
	t, ok := p.peekTok()
	if !ok {
		return nil, p.syn("")
	}
	if t.K == "id" {
		low := strings.ToLower(t.V)
		if low == "attribute_exists" || low == "attribute_not_exists" || low == "begins_with" || low == "contains" || low == "attribute_type" {
			nt, ok2 := p.peekAt(1)
			if ok2 && nt.K == "sym" && nt.V == "(" {
				p.i += 2
				var fargs []*operandArg
				if low == "attribute_exists" || low == "attribute_not_exists" {
					a, e := p.parseCmpOperand()
					if e != nil {
						return nil, e
					}
					fargs = []*operandArg{a}
				} else {
					a1, e := p.parseCmpOperand()
					if e != nil {
						return nil, e
					}
					if e := p.expectSym(","); e != nil {
						return nil, e
					}
					a2, e := p.parseCmpOperand()
					if e != nil {
						return nil, e
					}
					fargs = []*operandArg{a1, a2}
				}
				if e := p.expectSym(")"); e != nil {
					return nil, e
				}
				return &cnode{kind: "fn", fn: low, farg: fargs}, nil
			}
		}
	}
	lo, e := p.parseCmpOperand()
	if e != nil {
		return nil, e
	}
	if nt, ok2 := p.peekTok(); ok2 && nt.K == "sym" {
		switch nt.V {
		case "=", "<>", "<", "<=", ">", ">=":
			p.i++
			ro, e := p.parseCmpOperand()
			if e != nil {
				return nil, e
			}
			return &cnode{kind: "cmp", op: nt.V, lo: lo, ro: ro}, nil
		}
	}
	if p.isKeyword("BETWEEN") {
		p.i++
		a, e := p.parseCmpOperand()
		if e != nil {
			return nil, e
		}
		if !p.isKeyword("AND") {
			if tt, ok3 := p.peekTok(); ok3 {
				return nil, p.syn(tt.V)
			}
			return nil, p.syn("")
		}
		p.i++
		b, e := p.parseCmpOperand()
		if e != nil {
			return nil, e
		}
		return &cnode{kind: "between", lo: lo, midA: a, midB: b}, nil
	}
	if p.isKeyword("IN") {
		p.i++
		if e := p.expectSym("("); e != nil {
			return nil, e
		}
		var list []*operandArg
		for {
			a, e := p.parseInOperand()
			if e != nil {
				return nil, e
			}
			list = append(list, a)
			if p.hasSym(",") {
				p.i++
				continue
			}
			break
		}
		if e := p.expectSym(")"); e != nil {
			return nil, e
		}
		return &cnode{kind: "in", lo: lo, list: list}, nil
	}
	if nt, ok2 := p.peekTok(); ok2 {
		return nil, p.syn(nt.V)
	}
	return nil, p.syn("")
}

func (p *parser) parseCmpOperand() (*operandArg, *perr) {
	t, ok := p.nextTok()
	if !ok {
		return nil, p.syn("")
	}
	switch t.K {
	case "val":
		p.usedV[":"+t.V] = true
		return &operandArg{kind: "val", tok: ":" + t.V}, nil
	case "str":
		return nil, p.syn("'" + t.V + "'")
	case "ref":
		p.i--
		path, e := p.parseDocPath()
		if e != nil {
			return nil, e
		}
		return &operandArg{kind: "path", path: path}, nil
	case "id":
		nt, ok2 := p.peekTok()
		if strings.ToLower(t.V) == "size" && ok2 && nt.K == "sym" && nt.V == "(" {
			p.i++
			a, e := p.parseCmpOperand()
			if e != nil {
				return nil, e
			}
			if e := p.expectSym(")"); e != nil {
				return nil, e
			}
			return &operandArg{kind: "fn", fn: "size", arg: []*operandArg{a}}, nil
		}
		p.i--
		path, e := p.parseDocPath()
		if e != nil {
			return nil, e
		}
		return &operandArg{kind: "path", path: path}, nil
	}
	return nil, p.syn(t.V)
}

func (p *parser) parseInOperand() (*operandArg, *perr) {
	t, ok := p.nextTok()
	if !ok {
		return nil, p.syn("")
	}
	switch t.K {
	case "val":
		p.usedV[":"+t.V] = true
		return &operandArg{kind: "val", tok: ":" + t.V}, nil
	case "str":
		return &operandArg{kind: "lit", tok: t.V}, nil
	case "ref":
		p.i--
		path, e := p.parseDocPath()
		if e != nil {
			return nil, e
		}
		return &operandArg{kind: "path", path: path}, nil
	case "id":
		nt, ok2 := p.peekTok()
		if strings.ToLower(t.V) == "size" && ok2 && nt.K == "sym" && nt.V == "(" {
			p.i++
			a, e := p.parseCmpOperand()
			if e != nil {
				return nil, e
			}
			if e := p.expectSym(")"); e != nil {
				return nil, e
			}
			return &operandArg{kind: "fn", fn: "size", arg: []*operandArg{a}}, nil
		}
		p.i--
		path, e := p.parseDocPath()
		if e != nil {
			return nil, e
		}
		return &operandArg{kind: "path", path: path}, nil
	}
	return nil, p.syn(t.V)
}

func condForEachOperand(o *operandArg, f func(*operandArg)) {
	if o == nil {
		return
	}
	f(o)
	for _, x := range o.arg {
		condForEachOperand(x, f)
	}
}

func walkCondPaths(n *cnode, f func(docPath)) {
	if n == nil {
		return
	}
	op := func(o *operandArg) {
		condForEachOperand(o, func(x *operandArg) {
			if x.kind == "path" {
				f(x.path)
			}
		})
	}
	switch n.kind {
	case "logic":
		walkCondPaths(n.lf, f)
		walkCondPaths(n.rt, f)
	case "cmp":
		op(n.lo)
		op(n.ro)
	case "between":
		op(n.lo)
		op(n.midA)
		op(n.midB)
	case "in":
		op(n.lo)
		for _, o := range n.list {
			op(o)
		}
	case "fn":
		for _, o := range n.farg {
			op(o)
		}
	}
}

func condOperandVal(o *operandArg, doc map[string]*av, vals map[string]*av) (*av, bool, *apiErr) {
	switch o.kind {
	case "val":
		v := vals[o.tok]
		if v == nil {
			return nil, true, verr("An expression attribute value used in the condition expression is not defined; undefined value: " + o.tok)
		}
		return v, false, nil
	case "lit":
		return &av{T: "S", S: o.tok}, false, nil
	case "path":
		v, ok, e := walkDoc(doc, o.path, false)
		if e != nil {
			return nil, false, e
		}
		return v, !ok, nil
	case "fn":
		xv, xm, e := condOperandVal(o.arg[0], doc, vals)
		if e != nil {
			return nil, false, e
		}
		if xm {
			return nil, true, nil
		}
		n := 0
		switch xv.T {
		case "S":
			n = len(xv.S)
		case "B":
			n = len(xv.B)
		case "SS":
			n = len(xv.SS)
		case "NS":
			n = len(xv.NS)
		case "BS":
			n = len(xv.BS)
		case "M":
			n = len(xv.M)
		case "L":
			n = len(xv.L)
		default:
			return nil, false, verr("Incorrect operand type for operator or function; operator or function: SIZE, operand type: " + xv.T)
		}
		return &av{T: "N", N: strconv.Itoa(n)}, false, nil
	}
	return nil, true, verr("Unexpected expression operand")
}

func cmpOp(op string, x, y *av) (bool, *apiErr) {
	eq := avEqual(x, y)
	if op == "=" {
		return eq, nil
	}
	if op == "<>" {
		return !eq, nil
	}
	var c int
	ok := false
	switch {
	case x.T == "N" && y.T == "N":
		da, e1 := parseDecStr(x.N)
		db, e2 := parseDecStr(y.N)
		if e1 != nil {
			return false, e1
		}
		if e2 != nil {
			return false, e2
		}
		c = decCmp(da, db)
		ok = true
	case x.T == "S" && y.T == "S":
		c = strings.Compare(x.S, y.S)
		ok = true
	case x.T == "B" && y.T == "B":
		c = bytes.Compare(x.B, y.B)
		ok = true
	}
	if !ok {
		return false, nil
	}
	switch op {
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	}
	return false, nil
}

func ordCmp(x, y *av) (int, bool) {
	switch {
	case x.T == "N" && y.T == "N":
		da, e1 := parseDecStr(x.N)
		db, e2 := parseDecStr(y.N)
		if e1 != nil || e2 != nil {
			return 0, false
		}
		return decCmp(da, db), true
	case x.T == "S" && y.T == "S":
		return strings.Compare(x.S, y.S), true
	case x.T == "B" && y.T == "B":
		return bytes.Compare(x.B, y.B), true
	}
	return 0, false
}

func operandTypeDesc(o *operandArg, vals map[string]*av) string {
	switch o.kind {
	case "val":
		if v := vals[o.tok]; v != nil {
			return v.T
		}
		return "S"
	case "lit":
		return "S"
	case "fn":
		return strings.ToUpper(o.fn)
	}
	return "S"
}

func evalCond(n *cnode, doc map[string]*av, vals map[string]*av) (bool, *apiErr) {
	switch n.kind {
	case "logic":
		switch n.op {
		case "AND":
			l, e := evalCond(n.lf, doc, vals)
			if e != nil {
				return false, e
			}
			r, e := evalCond(n.rt, doc, vals)
			if e != nil {
				return false, e
			}
			return l && r, nil
		case "OR":
			l, e := evalCond(n.lf, doc, vals)
			if e != nil {
				return false, e
			}
			r, e := evalCond(n.rt, doc, vals)
			if e != nil {
				return false, e
			}
			return l || r, nil
		default:
			l, e := evalCond(n.lf, doc, vals)
			if e != nil {
				return false, e
			}
			return !l, nil
		}
	case "cmp":
		lv, lm, e := condOperandVal(n.lo, doc, vals)
		if e != nil {
			return false, e
		}
		rv, rm, e := condOperandVal(n.ro, doc, vals)
		if e != nil {
			return false, e
		}
		if lm || rm {
			return false, nil
		}
		return cmpOp(n.op, lv, rv)
	case "between":
		lv, lm, e := condOperandVal(n.lo, doc, vals)
		if e != nil {
			return false, e
		}
		lw, lwm, e := condOperandVal(n.midA, doc, vals)
		if e != nil {
			return false, e
		}
		hi, him, e := condOperandVal(n.midB, doc, vals)
		if e != nil {
			return false, e
		}
		if lm || lwm || him {
			return false, nil
		}
		c1, ok := ordCmp(lv, lw)
		if !ok {
			return false, nil
		}
		c2, ok2 := ordCmp(lv, hi)
		if !ok2 {
			return false, nil
		}
		return c1 >= 0 && c2 <= 0, nil
	case "in":
		lv, lm, e := condOperandVal(n.lo, doc, vals)
		if e != nil {
			return false, e
		}
		if lm {
			return false, nil
		}
		for _, o := range n.list {
			ov, om, e := condOperandVal(o, doc, vals)
			if e != nil {
				return false, e
			}
			if om {
				continue
			}
			if avEqual(lv, ov) {
				return true, nil
			}
		}
		return false, nil
	case "fn":
		switch n.fn {
		case "attribute_exists":
			a := n.farg[0]
			if a.kind != "path" {
				return false, verr("Incorrect operand type for operator or function; operator or function: ATTRIBUTE_EXISTS, operand type: " + operandTypeDesc(a, vals))
			}
			_, ok, e := walkDoc(doc, a.path, false)
			if e != nil {
				return false, e
			}
			return ok, nil
		case "attribute_not_exists":
			a := n.farg[0]
			if a.kind != "path" {
				return false, verr("Incorrect operand type for operator or function; operator or function: ATTRIBUTE_NOT_EXISTS, operand type: " + operandTypeDesc(a, vals))
			}
			_, ok, e := walkDoc(doc, a.path, false)
			if e != nil {
				return false, e
			}
			return !ok, nil
		case "begins_with":
			a := n.farg[0]
			if a.kind != "path" {
				return false, verr("Incorrect operand type for operator or function; operator or function: BEGINS_WITH, operand type: " + operandTypeDesc(a, vals))
			}
			xv, xm, e := condOperandVal(a, doc, vals)
			if e != nil {
				return false, e
			}
			yv, ym, e := condOperandVal(n.farg[1], doc, vals)
			if e != nil {
				return false, e
			}
			if xm || ym {
				return false, nil
			}
			if (xv.T == "S" || xv.T == "B") && yv.T == xv.T {
				if xv.T == "S" && strings.HasPrefix(xv.S, yv.S) {
					return true, nil
				}
				if xv.T == "B" && bytes.HasPrefix(xv.B, yv.B) {
					return true, nil
				}
				return false, nil
			}
			return false, verr("Incorrect operand type for operator or function; operator or function: BEGINS_WITH, operand type: " + xv.T)
		case "contains":
			xv, xm, e := condOperandVal(n.farg[0], doc, vals)
			if e != nil {
				return false, e
			}
			if xm {
				return false, nil
			}
			yv, ym, e := condOperandVal(n.farg[1], doc, vals)
			if e != nil {
				return false, e
			}
			if ym {
				return false, nil
			}
			switch xv.T {
			case "S":
				if yv.T == "S" && strings.Contains(xv.S, yv.S) {
					return true, nil
				}
				return false, nil
			case "B":
				if yv.T == "B" && bytes.Contains(xv.B, yv.B) {
					return true, nil
				}
				return false, nil
			case "SS":
				if yv.T != "S" {
					return false, nil
				}
				for _, s := range xv.SS {
					if s == yv.S {
						return true, nil
					}
				}
				return false, nil
			case "NS":
				if yv.T != "N" {
					return false, nil
				}
				yd, e2 := parseDecStr(yv.N)
				if e2 != nil {
					return false, nil
				}
				for _, s := range xv.NS {
					if d, e3 := parseDecStr(s); e3 == nil && decCmp(d, yd) == 0 {
						return true, nil
					}
				}
				return false, nil
			case "BS":
				if yv.T != "B" {
					return false, nil
				}
				for _, b := range xv.BS {
					if bytes.Equal(b, yv.B) {
						return true, nil
					}
				}
				return false, nil
			case "L":
				for _, el := range xv.L {
					if avEqual(el, yv) {
						return true, nil
					}
				}
				return false, nil
			default:
				return false, verr("Incorrect operand type for operator or function; operator or function: CONTAINS, operand type: " + xv.T)
			}
		case "attribute_type":
			a := n.farg[0]
			if a.kind != "path" {
				return false, verr("Incorrect operand type for operator or function; operator or function: ATTRIBUTE_TYPE, operand type: " + operandTypeDesc(a, vals))
			}
			xv, xm, e := condOperandVal(a, doc, vals)
			if e != nil {
				return false, e
			}
			if xm {
				return false, nil
			}
			yv, ym, e := condOperandVal(n.farg[1], doc, vals)
			if e != nil {
				return false, e
			}
			if ym {
				return false, nil
			}
			if yv.T != "S" {
				return false, verr("Incorrect operand type for operator or function; operator or function: ATTRIBUTE_TYPE, operand type: " + yv.T)
			}
			return xv.T == yv.S, nil
		}
	}
	return false, verr("Unexpected condition function")
}

// ----- projection expression ------------------------------------------------

func parseProjection(src string) ([]docPath, map[string]bool, *perr) {
	toks, e := lex(src, kProj)
	if e != nil {
		return nil, nil, e
	}
	p := &parser{toks: toks, kind: kProj, usedN: map[string]bool{}, usedV: map[string]bool{}}
	if len(toks) == 0 {
		return nil, nil, p.syn("")
	}
	var out []docPath
	for {
		pp, e := p.parseDocPath()
		if e != nil {
			return nil, nil, e
		}
		out = append(out, pp)
		if p.hasSym(",") {
			p.i++
			continue
		}
		break
	}
	if t, ok := p.peekTok(); ok {
		return nil, nil, p.syn(t.V)
	}
	return out, p.usedN, nil
}

// ---- nested result building (UPDATED_* values, projections) -----------------

type nestNode struct {
	m      map[string]*nestNode
	l      map[int]*nestNode
	val    any
	hasVal bool
}

func newNestNode() *nestNode {
	return &nestNode{m: map[string]*nestNode{}, l: map[int]*nestNode{}}
}

func nestBuildAny(doc map[string]*av, paths []docPath) (map[string]any, *apiErr) {
	root := newNestNode()
	for _, p := range paths {
		v, ok, aerr := walkDoc(doc, p, false)
		if aerr != nil {
			return nil, aerr
		}
		if !ok {
			continue
		}
		cur := root
		for _, el := range p {
			if el.nameStr != "" {
				nx, found := cur.m[el.nameStr]
				if !found {
					nx = newNestNode()
					cur.m[el.nameStr] = nx
				}
				cur = nx
			}
			for _, ix := range el.idx {
				nx, found := cur.l[ix]
				if !found {
					nx = newNestNode()
					cur.l[ix] = nx
				}
				cur = nx
			}
		}
		cur.val = avToAny(v)
		cur.hasVal = true
	}
	out := make(map[string]any, len(root.m))
	for k, nn := range root.m {
		out[k] = nestResolve(nn)
	}
	return out, nil
}

func nestResolve(n *nestNode) any {
	if n.hasVal {
		return n.val
	}
	if len(n.l) > 0 {
		keys := make([]int, 0, len(n.l))
		for k := range n.l {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		arr := make([]any, 0, len(keys))
		for _, k := range keys {
			arr = append(arr, nestResolve(n.l[k]))
		}
		return map[string]any{"L": arr}
	}
	if len(n.m) > 0 {
		mm := make(map[string]any, len(n.m))
		for k, nn := range n.m {
			mm[k] = nestResolve(nn)
		}
		return map[string]any{"M": mm}
	}
	return map[string]any{}
}

// ---------------------------------------------------------------------------
// Item size / capacity
// ---------------------------------------------------------------------------

func avSize(a *av) int {
	switch a.T {
	case "S":
		return len(a.S)
	case "B":
		return len(a.B)
	case "N":
		return len(a.N)/2 + 2
	case "BOOL", "NULL":
		return 1
	case "SS":
		s := 3
		for _, e := range a.SS {
			s += len(e)
		}
		return s
	case "NS":
		s := 3
		for _, e := range a.NS {
			s += len(e)/2 + 1
		}
		return s
	case "BS":
		s := 3
		for _, e := range a.BS {
			s += len(e)
		}
		return s
	case "M":
		s := 3
		for k, v := range a.M {
			s += len(k) + avSize(v)
		}
		return s
	case "L":
		s := 3
		for _, v := range a.L {
			s += avSize(v)
		}
		return s
	}
	return 0
}

func docSize(d map[string]*av) int {
	s := 0
	for k, v := range d {
		s += len(k) + avSize(v)
	}
	return s
}

func writeUnits(sz int) int {
	n := (sz + 1023) / 1024
	if n < 1 {
		n = 1
	}
	return n
}

func floatNum(f float64) json.Number {
	return json.Number(strconv.FormatFloat(f, 'f', 1, 64))
}

// ---------------------------------------------------------------------------
// Table storage
// ---------------------------------------------------------------------------

type keyElem struct {
	Name  string
	Type  string
	Range bool
}

type attrDef struct {
	Name string
	Type string
}

type table struct {
	Name      string
	KeySchema []keyElem
	AttrDefs  []attrDef
	Billing   string
	RCU, WCU  int
	Items     map[string]map[string]*av
	Created   time.Time
	LSIAny    []any
	GSIAny    []any
	StreamRaw json.RawMessage
}

type dbstore struct {
	mu     sync.Mutex
	tables map[string]*table
}

var store = &dbstore{tables: map[string]*table{}}

func (s *dbstore) getTable(name string) *table {
	t, ok := s.tables[name]
	if !ok {
		return nil
	}
	return t
}

func (t *table) keyStringFromDoc(keyDoc map[string]*av) string {
	parts := make([]string, 0, len(t.KeySchema))
	for _, ke := range t.KeySchema {
		b, _ := json.Marshal(avToAny(keyDoc[ke.Name]))
		parts = append(parts, ke.Name+"="+string(b))
	}
	return strings.Join(parts, string(rune(31)))
}

func (t *table) find(keyDoc map[string]*av) map[string]*av {
	if it, ok := t.Items[t.keyStringFromDoc(keyDoc)]; ok {
		return it
	}
	return nil
}

func (t *table) checkKey(keyDoc map[string]*av) *apiErr {
	for _, ke := range t.KeySchema {
		v, ok := keyDoc[ke.Name]
		if !ok || v == nil {
			return verr("One or more parameter values were invalid: Missing the key " + ke.Name + " in the item")
		}
		if v.T != ke.Type {
			return verr("One or more parameter values were invalid: Type mismatch for Key Schema Element: " + ke.Name + " Expected: " + ke.Type + " Actual: " + v.T)
		}
		if v.T == "S" && v.S == "" {
			return verr("One or more parameter values are not valid: The AttributeValue for a key attribute cannot contain an empty string value.")
		}
		if v.T == "B" && len(v.B) == 0 {
			return verr("One or more parameter values are not valid: The AttributeValue for a key attribute cannot contain an empty binary value.")
		}
	}
	return nil
}

func (t *table) describe(status string) map[string]any {
	total := 0
	for _, it := range t.Items {
		total += docSize(it)
	}
	ad := []any{}
	for _, a := range t.AttrDefs {
		ad = append(ad, map[string]any{"AttributeName": a.Name, "AttributeType": a.Type})
	}
	ksArr := []any{}
	for _, k := range t.KeySchema {
		kt := "HASH"
		if k.Range {
			kt = "RANGE"
		}
		ksArr = append(ksArr, map[string]any{"AttributeName": k.Name, "KeyType": kt})
	}
	created := json.Number(strconv.FormatFloat(math.Round(float64(t.Created.UnixNano())/1e6)/1000, 'f', 3, 64))
	lsi := t.LSIAny
	if lsi == nil {
		lsi = []any{}
	}
	d := map[string]any{
		"AttributeDefinitions":  ad,
		"CreationDateTime":      created,
		"ItemCount":             len(t.Items),
		"KeySchema":             ksArr,
		"LocalSecondaryIndexes": lsi,
		"ProvisionedThroughput": map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": t.RCU, "WriteCapacityUnits": t.WCU},
		"TableArn":              "arn:aws:dynamodb:ddblocal:000000000000:table/" + t.Name,
		"TableName":             t.Name,
		"TableSizeBytes":        total,
		"TableStatus":           status,
	}
	if t.Billing == "PAY_PER_REQUEST" {
		d["BillingModeSummary"] = map[string]any{"BillingMode": "PAY_PER_REQUEST"}
	}
	if t.GSIAny != nil {
		d["GlobalSecondaryIndexes"] = t.GSIAny
	}
	if t.StreamRaw != nil {
		var v any
		if err := json.Unmarshal(t.StreamRaw, &v); err == nil {
			d["StreamSpecification"] = v
		}
	}
	return d
}

// ---------------------------------------------------------------------------
// Shared condition helpers
// ---------------------------------------------------------------------------

func prepareCondition(src *string, names map[string]string, vals map[string]*av) (*cnode, *apiErr) {
	if src == nil {
		return nil, nil
	}
	c, perr := parseCondExpr(*src)
	if perr != nil {
		return nil, perr.api()
	}
	var perr0 *perr
	walkCondPaths(c, func(p docPath) {
		if perr0 == nil {
			perr0 = resolvePathNames(p, names, "condition expression")
		}
	})
	if perr0 != nil {
		return nil, perr0.api()
	}
	for k := range c.usedV {
		if _, ok := vals[k]; !ok {
			return nil, (&perr{noPrefix: true, msg: "An expression attribute value used in the condition expression is not defined; undefined value: " + k}).api()
		}
	}
	return c, nil
}

func evalCondition(c *cnode, doc map[string]*av, vals map[string]*av) (bool, *apiErr) {
	if c == nil {
		return true, nil
	}
	return evalCond(c, doc, vals)
}

func checkUsedPlaceholders(names map[string]string, vals map[string]*av, usedN, usedV map[string]bool) *apiErr {
	var prov []string
	for k := range names {
		prov = append(prov, k)
	}
	sort.Strings(prov)
	var un []string
	for _, k := range prov {
		if !usedN[k] {
			un = append(un, k)
		}
	}
	if len(un) > 0 {
		return verr("Value provided in ExpressionAttributeNames unused in expressions: keys: {" + strings.Join(un, ", ") + "}")
	}
	var pv []string
	for k := range vals {
		pv = append(pv, k)
	}
	sort.Strings(pv)
	var uv []string
	for _, k := range pv {
		if !usedV[k] {
			uv = append(uv, k)
		}
	}
	if len(uv) > 0 {
		return verr("Value provided in ExpressionAttributeValues unused in expressions: keys: {" + strings.Join(uv, ", ") + "}")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func hUpdateItem(body []byte) (any, *apiErr) {
	var req struct {
		TableName                              string                     `json:"TableName"`
		Key                                    map[string]json.RawMessage `json:"Key"`
		UpdateExpression                       *string                    `json:"UpdateExpression"`
		ConditionExpression                    *string                    `json:"ConditionExpression"`
		ExpressionAttributeNames               map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues              map[string]json.RawMessage `json:"ExpressionAttributeValues"`
		ReturnValues                           string                     `json:"ReturnValues"`
		ReturnConsumedCapacity                 string                     `json:"ReturnConsumedCapacity"`
		ReturnItemCollectionMetrics            string                     `json:"ReturnItemCollectionMetrics"`
		ReturnValuesOnConditionCheckExpression string                     `json:"ReturnValuesOnConditionCheckExpression"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	if len(req.TableName) < 3 || len(req.TableName) > 255 {
		return nil, verr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	switch req.ReturnValues {
	case "", "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW":
	default:
		return nil, enumFailErr("returnValues", req.ReturnValues, "ALL_NEW, ALL_OLD, NONE, UPDATED_NEW, UPDATED_OLD")
	}
	switch req.ReturnConsumedCapacity {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, enumFailErr("returnConsumedCapacity", req.ReturnConsumedCapacity, "INDEXES, NONE, TOTAL")
	}
	switch req.ReturnItemCollectionMetrics {
	case "", "NONE", "SIZE":
	default:
		return nil, enumFailErr("returnItemCollectionMetrics", req.ReturnItemCollectionMetrics, "NONE, SIZE")
	}
	switch req.ReturnValuesOnConditionCheckExpression {
	case "", "NONE", "ALL_OLD":
	default:
		return nil, enumFailErr("returnValuesOnConditionCheckExpression", req.ReturnValuesOnConditionCheckExpression, "ALL_OLD, NONE")
	}
	vals := map[string]*av{}
	for k, raw := range req.ExpressionAttributeValues {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		vals[k] = a
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	if req.Key == nil {
		return nil, requiredNullErr("key")
	}
	keyDoc := map[string]*av{}
	for k, raw := range req.Key {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		keyDoc[k] = a
	}
	if aerr := t.checkKey(keyDoc); aerr != nil {
		return nil, aerr
	}
	existing := t.find(keyDoc)

	var uex *updateExpr
	if req.UpdateExpression != nil {
		u, perr := parseUpdateExpr(*req.UpdateExpression)
		if perr != nil {
			return nil, perr.api()
		}
		uex = u
		for _, p := range uexAllPaths(uex) {
			if e := resolvePathNames(p, req.ExpressionAttributeNames, "update expression"); e != nil {
				return nil, e.api()
			}
		}
		for k := range uex.usedV {
			if _, ok := vals[k]; !ok {
				return nil, (&perr{noPrefix: true, msg: "An expression attribute value used in the update expression is not defined; undefined value: " + k}).api()
			}
		}
	}

	cond, aerr := prepareCondition(req.ConditionExpression, req.ExpressionAttributeNames, vals)
	if aerr != nil {
		return nil, aerr
	}

	mergedUsedN := map[string]bool{}
	mergedUsedV := map[string]bool{}
	if uex != nil {
		for k := range uex.usedN {
			mergedUsedN[k] = true
		}
		for k := range uex.usedV {
			mergedUsedV[k] = true
		}
	}
	if cond != nil {
		for k := range cond.usedN {
			mergedUsedN[k] = true
		}
		for k := range cond.usedV {
			mergedUsedV[k] = true
		}
	}
	if aerr := checkUsedPlaceholders(req.ExpressionAttributeNames, vals, mergedUsedN, mergedUsedV); aerr != nil {
		return nil, aerr
	}

	condDoc := map[string]*av{}
	if existing != nil {
		condDoc = existing
	}
	ok, aerr := evalCondition(cond, condDoc, vals)
	if aerr != nil {
		return nil, aerr
	}
	if !ok {
		var oldForFail map[string]*av
		if req.ReturnValuesOnConditionCheckExpression == "ALL_OLD" {
			oldForFail = existing
		}
		return nil, condFailedErr(oldForFail)
	}

	keyNames := map[string]bool{}
	for _, ke := range t.KeySchema {
		keyNames[ke.Name] = true
	}

	base := map[string]*av{}
	if existing != nil {
		base = docClone(existing)
	} else {
		for _, ke := range t.KeySchema {
			base[ke.Name] = avClone(keyDoc[ke.Name])
		}
	}
	orig := base
	cur := base
	leafPaths := []docPath{}
	if uex != nil && uex.hasAny {
		allPaths := uexActionPaths(uex)
		for _, p := range allPaths {
			if keyNames[p[0].nameStr] {
				return nil, verr("One or more parameter values were invalid: Cannot update attribute " + p[0].nameStr + ". This attribute is part of the key")
			}
		}
		for i := 0; i < len(allPaths); i++ {
			for j := i + 1; j < len(allPaths); j++ {
				if docPathOverlaps(allPaths[i], allPaths[j]) {
					return nil, verr("Two document paths overlap with each other")
				}
			}
		}
		leafPaths = allPaths
		orig = docClone(base)
		cur = docClone(base)
		for i := range uex.sets {
			act := &uex.sets[i]
			val, aerr := resolveUpdOperand(act.rhs, orig, vals)
			if aerr != nil {
				return nil, aerr
			}
			if aerr := setLeaf(cur, act.path, val); aerr != nil {
				return nil, aerr
			}
		}
		for _, p := range uex.removes {
			if aerr := removeLeaf(cur, p); aerr != nil {
				return nil, aerr
			}
		}
		for i := range uex.adds {
			act := &uex.adds[i]
			if aerr := addLeaf(cur, act.path, avClone(vals[act.val])); aerr != nil {
				return nil, aerr
			}
		}
		for i := range uex.deletes {
			act := &uex.deletes[i]
			if aerr := deleteLeaf(cur, act.path, avClone(vals[act.val])); aerr != nil {
				return nil, aerr
			}
		}
	}

	t.Items[t.keyStringFromDoc(keyDoc)] = cur

	resp := map[string]any{}
	switch req.ReturnValues {
	case "ALL_OLD":
		if existing != nil {
			resp["Attributes"] = docToAny(existing)
		}
	case "ALL_NEW":
		resp["Attributes"] = docToAny(cur)
	case "UPDATED_OLD":
		built, aerr := nestBuildAny(orig, leafPaths)
		if aerr != nil {
			return nil, aerr
		}
		if len(built) > 0 {
			resp["Attributes"] = built
		}
	case "UPDATED_NEW":
		built, aerr := nestBuildAny(cur, leafPaths)
		if aerr != nil {
			return nil, aerr
		}
		if len(built) > 0 {
			resp["Attributes"] = built
		}
	}
	if rc := req.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
		units := floatNum(float64(writeUnits(docSize(cur))))
		cc := map[string]any{"TableName": req.TableName, "CapacityUnits": units}
		if rc == "INDEXES" {
			cc["Table"] = map[string]any{"CapacityUnits": units}
		}
		resp["ConsumedCapacity"] = cc
	}
	if req.ReturnItemCollectionMetrics == "SIZE" {
		keyOnly := map[string]any{}
		for _, ke := range t.KeySchema {
			keyOnly[ke.Name] = avToAny(keyDoc[ke.Name])
		}
		resp["ItemCollectionMetrics"] = map[string]any{"ItemCollectionKey": keyOnly, "SizeEstimateGB": json.Number("0.0")}
	}
	return resp, nil
}

func hPutItem(body []byte) (any, *apiErr) {
	var req struct {
		TableName                              string                     `json:"TableName"`
		Item                                   map[string]json.RawMessage `json:"Item"`
		ConditionExpression                    *string                    `json:"ConditionExpression"`
		ExpressionAttributeNames               map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues              map[string]json.RawMessage `json:"ExpressionAttributeValues"`
		ReturnValues                           string                     `json:"ReturnValues"`
		ReturnConsumedCapacity                 string                     `json:"ReturnConsumedCapacity"`
		ReturnValuesOnConditionCheckExpression string                     `json:"ReturnValuesOnConditionCheckExpression"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	if len(req.TableName) < 3 || len(req.TableName) > 255 {
		return nil, verr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	switch req.ReturnValues {
	case "", "NONE", "ALL_OLD":
	default:
		return nil, enumFailErr("returnValues", req.ReturnValues, "ALL_OLD, NONE")
	}
	switch req.ReturnConsumedCapacity {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, enumFailErr("returnConsumedCapacity", req.ReturnConsumedCapacity, "INDEXES, NONE, TOTAL")
	}
	switch req.ReturnValuesOnConditionCheckExpression {
	case "", "NONE", "ALL_OLD":
	default:
		return nil, enumFailErr("returnValuesOnConditionCheckExpression", req.ReturnValuesOnConditionCheckExpression, "ALL_OLD, NONE")
	}
	vals := map[string]*av{}
	for k, raw := range req.ExpressionAttributeValues {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		vals[k] = a
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	if req.Item == nil {
		return nil, requiredNullErr("item")
	}
	item := map[string]*av{}
	for k, raw := range req.Item {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		item[k] = a
	}
	keyDoc := map[string]*av{}
	for _, ke := range t.KeySchema {
		v, ok := item[ke.Name]
		if !ok {
			return nil, verr("One or more parameter values were invalid: Missing the key " + ke.Name + " in the item")
		}
		keyDoc[ke.Name] = v
	}
	if aerr := t.checkKey(keyDoc); aerr != nil {
		return nil, aerr
	}
	cond, aerr := prepareCondition(req.ConditionExpression, req.ExpressionAttributeNames, vals)
	if aerr != nil {
		return nil, aerr
	}
	usedN, usedV := map[string]bool{}, map[string]bool{}
	if cond != nil {
		for k := range cond.usedN {
			usedN[k] = true
		}
		for k := range cond.usedV {
			usedV[k] = true
		}
	}
	if aerr := checkUsedPlaceholders(req.ExpressionAttributeNames, vals, usedN, usedV); aerr != nil {
		return nil, aerr
	}
	existing := t.find(keyDoc)
	condDoc := map[string]*av{}
	if existing != nil {
		condDoc = existing
	}
	ok, aerr := evalCondition(cond, condDoc, vals)
	if aerr != nil {
		return nil, aerr
	}
	if !ok {
		var old map[string]*av
		if req.ReturnValuesOnConditionCheckExpression == "ALL_OLD" {
			old = existing
		}
		return nil, condFailedErr(old)
	}
	old := existing
	t.Items[t.keyStringFromDoc(keyDoc)] = item
	resp := map[string]any{}
	if req.ReturnValues == "ALL_OLD" && old != nil {
		resp["Attributes"] = docToAny(old)
	}
	if rc := req.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
		units := floatNum(float64(writeUnits(docSize(item))))
		cc := map[string]any{"TableName": req.TableName, "CapacityUnits": units}
		if rc == "INDEXES" {
			cc["Table"] = map[string]any{"CapacityUnits": units}
		}
		resp["ConsumedCapacity"] = cc
	}
	return resp, nil
}

func hGetItem(body []byte) (any, *apiErr) {
	var req struct {
		TableName                string                     `json:"TableName"`
		Key                      map[string]json.RawMessage `json:"Key"`
		ProjectionExpression     *string                    `json:"ProjectionExpression"`
		ExpressionAttributeNames map[string]string          `json:"ExpressionAttributeNames"`
		ConsistentRead           bool                       `json:"ConsistentRead"`
		ReturnConsumedCapacity   string                     `json:"ReturnConsumedCapacity"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	if len(req.TableName) < 3 || len(req.TableName) > 255 {
		return nil, verr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	switch req.ReturnConsumedCapacity {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, enumFailErr("returnConsumedCapacity", req.ReturnConsumedCapacity, "INDEXES, NONE, TOTAL")
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	if req.Key == nil {
		return nil, requiredNullErr("key")
	}
	keyDoc := map[string]*av{}
	for k, raw := range req.Key {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		keyDoc[k] = a
	}
	if aerr := t.checkKey(keyDoc); aerr != nil {
		return nil, aerr
	}
	usedN := map[string]bool{}
	var proj []docPath
	if req.ProjectionExpression != nil {
		paths, un, perr := parseProjection(*req.ProjectionExpression)
		if perr != nil {
			return nil, perr.api()
		}
		for i := range paths {
			if e := resolvePathNames(paths[i], req.ExpressionAttributeNames, "projection expression"); e != nil {
				return nil, e.api()
			}
		}
		for i := 0; i < len(paths); i++ {
			for j := i + 1; j < len(paths); j++ {
				if docPathOverlaps(paths[i], paths[j]) {
					return nil, (&perr{kind: kProj, msg: "Two document paths overlap with each other"}).api()
				}
			}
		}
		proj = paths
		usedN = un
	}
	if aerr := checkUsedPlaceholders(req.ExpressionAttributeNames, map[string]*av{}, usedN, map[string]bool{}); aerr != nil {
		return nil, aerr
	}
	existing := t.find(keyDoc)
	resp := map[string]any{}
	if existing != nil {
		if proj == nil {
			resp["Item"] = docToAny(existing)
		} else {
			built, aerr := nestBuildAny(existing, proj)
			if aerr != nil {
				return nil, aerr
			}
			resp["Item"] = built
		}
	}
	if rc := req.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
		sz := 0
		if existing != nil {
			sz = docSize(existing)
		}
		nRcu := (sz + 4095) / 4096
		if nRcu < 1 {
			nRcu = 1
		}
		u := float64(nRcu)
		if !req.ConsistentRead {
			u = float64(nRcu) * 0.5
			if u < 0.5 {
				u = 0.5
			}
		}
		units := floatNum(u)
		cc := map[string]any{"TableName": req.TableName, "CapacityUnits": units}
		if rc == "INDEXES" {
			cc["Table"] = map[string]any{"CapacityUnits": units}
		}
		resp["ConsumedCapacity"] = cc
	}
	return resp, nil
}

func hDeleteItem(body []byte) (any, *apiErr) {
	var req struct {
		TableName                              string                     `json:"TableName"`
		Key                                    map[string]json.RawMessage `json:"Key"`
		ConditionExpression                    *string                    `json:"ConditionExpression"`
		ExpressionAttributeNames               map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues              map[string]json.RawMessage `json:"ExpressionAttributeValues"`
		ReturnValues                           string                     `json:"ReturnValues"`
		ReturnConsumedCapacity                 string                     `json:"ReturnConsumedCapacity"`
		ReturnValuesOnConditionCheckExpression string                     `json:"ReturnValuesOnConditionCheckExpression"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	if len(req.TableName) < 3 || len(req.TableName) > 255 {
		return nil, verr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	switch req.ReturnValues {
	case "", "NONE", "ALL_OLD":
	default:
		return nil, enumFailErr("returnValues", req.ReturnValues, "ALL_OLD, NONE")
	}
	switch req.ReturnConsumedCapacity {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, enumFailErr("returnConsumedCapacity", req.ReturnConsumedCapacity, "INDEXES, NONE, TOTAL")
	}
	switch req.ReturnValuesOnConditionCheckExpression {
	case "", "NONE", "ALL_OLD":
	default:
		return nil, enumFailErr("returnValuesOnConditionCheckExpression", req.ReturnValuesOnConditionCheckExpression, "ALL_OLD, NONE")
	}
	vals := map[string]*av{}
	for k, raw := range req.ExpressionAttributeValues {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		vals[k] = a
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	if req.Key == nil {
		return nil, requiredNullErr("key")
	}
	keyDoc := map[string]*av{}
	for k, raw := range req.Key {
		a, aerr := parseAV(raw)
		if aerr != nil {
			return nil, aerr
		}
		keyDoc[k] = a
	}
	if aerr := t.checkKey(keyDoc); aerr != nil {
		return nil, aerr
	}
	cond, aerr := prepareCondition(req.ConditionExpression, req.ExpressionAttributeNames, vals)
	if aerr != nil {
		return nil, aerr
	}
	usedN, usedV := map[string]bool{}, map[string]bool{}
	if cond != nil {
		for k := range cond.usedN {
			usedN[k] = true
		}
		for k := range cond.usedV {
			usedV[k] = true
		}
	}
	if aerr := checkUsedPlaceholders(req.ExpressionAttributeNames, vals, usedN, usedV); aerr != nil {
		return nil, aerr
	}
	existing := t.find(keyDoc)
	condDoc := map[string]*av{}
	if existing != nil {
		condDoc = existing
	}
	ok, aerr := evalCondition(cond, condDoc, vals)
	if aerr != nil {
		return nil, aerr
	}
	if !ok {
		var old map[string]*av
		if req.ReturnValuesOnConditionCheckExpression == "ALL_OLD" {
			old = existing
		}
		return nil, condFailedErr(old)
	}
	keyStr := t.keyStringFromDoc(keyDoc)
	units := 1
	resp := map[string]any{}
	if old := t.Items[keyStr]; old != nil {
		units = writeUnits(docSize(old))
		delete(t.Items, keyStr)
		if req.ReturnValues == "ALL_OLD" {
			resp["Attributes"] = docToAny(old)
		}
	}
	if rc := req.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
		un := floatNum(float64(units))
		cc := map[string]any{"TableName": req.TableName, "CapacityUnits": un}
		if rc == "INDEXES" {
			cc["Table"] = map[string]any{"CapacityUnits": un}
		}
		resp["ConsumedCapacity"] = cc
	}
	return resp, nil
}

func hBatchWriteItem(body []byte) (any, *apiErr) {
	var req struct {
		RequestItems                map[string][]json.RawMessage `json:"RequestItems"`
		ReturnItemCollectionMetrics string                       `json:"ReturnItemCollectionMetrics"`
		ReturnConsumedCapacity      string                       `json:"ReturnConsumedCapacity"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	switch req.ReturnConsumedCapacity {
	case "", "NONE", "TOTAL", "INDEXES":
	default:
		return nil, enumFailErr("returnConsumedCapacity", req.ReturnConsumedCapacity, "INDEXES, NONE, TOTAL")
	}
	switch req.ReturnItemCollectionMetrics {
	case "", "NONE", "SIZE":
	default:
		return nil, enumFailErr("returnItemCollectionMetrics", req.ReturnItemCollectionMetrics, "NONE, SIZE")
	}
	if req.RequestItems == nil {
		return nil, requiredNullErr("requestItems")
	}
	for name := range req.RequestItems {
		if store.getTable(name) == nil {
			return nil, tableNotFoundErr(name)
		}
	}
	perTable := map[string]int{}
	for name, entries := range req.RequestItems {
		if len(entries) > 25 {
			return nil, verr("One or more parameter values were invalid: Number of items in a single request must be less than or equal to: 25")
		}
		t := store.getTable(name)
		for _, raw := range entries {
			var wr map[string]json.RawMessage
			if err := json.Unmarshal(raw, &wr); err != nil {
				return nil, serErr()
			}
			var units int
			if pr, ok := wr["PutRequest"]; ok {
				var prq struct {
					Item map[string]json.RawMessage `json:"Item"`
				}
				if err := json.Unmarshal(pr, &prq); err != nil {
					return nil, serErr()
				}
				if prq.Item == nil {
					return nil, requiredNullErr("item")
				}
				item := map[string]*av{}
				for k, rv := range prq.Item {
					a, aerr := parseAV(rv)
					if aerr != nil {
						return nil, aerr
					}
					item[k] = a
				}
				keyDoc := map[string]*av{}
				for _, ke := range t.KeySchema {
					v, ok2 := item[ke.Name]
					if !ok2 {
						return nil, verr("One or more parameter values were invalid: Missing the key " + ke.Name + " in the item")
					}
					keyDoc[ke.Name] = v
				}
				if aerr := t.checkKey(keyDoc); aerr != nil {
					return nil, aerr
				}
				t.Items[t.keyStringFromDoc(keyDoc)] = item
				units = writeUnits(docSize(item))
			} else if dr, ok := wr["DeleteRequest"]; ok {
				var drq struct {
					Key map[string]json.RawMessage `json:"Key"`
				}
				if err := json.Unmarshal(dr, &drq); err != nil {
					return nil, serErr()
				}
				keyDoc := map[string]*av{}
				for k, rv := range drq.Key {
					a, aerr := parseAV(rv)
					if aerr != nil {
						return nil, aerr
					}
					keyDoc[k] = a
				}
				if aerr := t.checkKey(keyDoc); aerr != nil {
					return nil, aerr
				}
				keyStr := t.keyStringFromDoc(keyDoc)
				if oldv := t.Items[keyStr]; oldv != nil {
					units = writeUnits(docSize(oldv))
					delete(t.Items, keyStr)
				} else {
					units = 1
				}
			} else {
				return nil, verr("One or more parameter values were invalid: The request should contain either PutRequest or DeleteRequest")
			}
			perTable[name] += units
		}
	}
	resp := map[string]any{"UnprocessedItems": map[string]any{}}
	if rc := req.ReturnConsumedCapacity; rc == "TOTAL" || rc == "INDEXES" {
		names := []string{}
		for n := range perTable {
			names = append(names, n)
		}
		sort.Strings(names)
		ccs := []any{}
		for _, n := range names {
			entry := map[string]any{"TableName": n, "CapacityUnits": floatNum(float64(perTable[n]))}
			if rc == "INDEXES" {
				entry["Table"] = map[string]any{"CapacityUnits": floatNum(float64(perTable[n]))}
			}
			ccs = append(ccs, entry)
		}
		resp["ConsumedCapacity"] = ccs
	}
	return resp, nil
}

func hCreateTable(body []byte) (any, *apiErr) {
	var req struct {
		TableName            string `json:"TableName"`
		AttributeDefinitions []struct {
			AttributeName string `json:"AttributeName"`
			AttributeType string `json:"AttributeType"`
		} `json:"AttributeDefinitions"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		BillingMode           string `json:"BillingMode"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  *int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits *int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
		LocalSecondaryIndexes  []json.RawMessage `json:"LocalSecondaryIndexes"`
		GlobalSecondaryIndexes []json.RawMessage `json:"GlobalSecondaryIndexes"`
		StreamSpecification    json.RawMessage   `json:"StreamSpecification"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	if len(req.TableName) < 3 || len(req.TableName) > 255 {
		return nil, verr("TableName must be at least 3 characters long and at most 255 characters long")
	}
	if req.KeySchema == nil {
		return nil, requiredNullErr("keySchema")
	}
	if req.AttributeDefinitions == nil {
		return nil, requiredNullErr("attributeDefinitions")
	}
	defs := map[string]string{}
	var adList []attrDef
	for _, a := range req.AttributeDefinitions {
		if a.AttributeName == "" {
			return nil, verr("One or more parameter values were invalid: Attribute name cannot be empty")
		}
		if a.AttributeType != "S" && a.AttributeType != "N" && a.AttributeType != "B" {
			return nil, enumFailErr("attributeDefinitions.member.attributeType", a.AttributeType, "B, N, S")
		}
		if _, dup := defs[a.AttributeName]; dup {
			return nil, verr("One or more parameter values were invalid: AttributeDefinitions cannot contain duplicate attribute names")
		}
		defs[a.AttributeName] = a.AttributeType
		adList = append(adList, attrDef{Name: a.AttributeName, Type: a.AttributeType})
	}
	if len(req.KeySchema) > 2 {
		return nil, verr("One or more parameter values were invalid: The KeySchema can only contain a maximum of 2 key attributes")
	}
	keys := []keyElem{}
	seenK := map[string]bool{}
	for idx, ks := range req.KeySchema {
		if ks.KeyType != "HASH" && ks.KeyType != "RANGE" && ks.KeyType != "SORT" {
			return nil, enumFailErr("keySchema.member.keyType", ks.KeyType, "HASH, RANGE")
		}
		if seenK[ks.AttributeName] {
			return nil, verr("One or more parameter values were invalid: Duplicate key attribute names are not allowed")
		}
		seenK[ks.AttributeName] = true
		if idx == 0 && ks.KeyType != "HASH" {
			return nil, verr("One or more parameter values were invalid: The first KeySchema element is not a HASH key type")
		}
		if idx == 1 && ks.KeyType == "HASH" {
			return nil, verr("One or more parameter values were invalid: Only one HASH key type can be specified")
		}
		typ, ok := defs[ks.AttributeName]
		if !ok {
			return nil, verr("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [" + ks.AttributeName + "]")
		}
		keys = append(keys, keyElem{Name: ks.AttributeName, Type: typ, Range: ks.KeyType != "HASH"})
	}
	if len(keys) == 0 {
		return nil, verr("One or more parameter values were invalid: KeySchema cannot be empty")
	}
	billing := req.BillingMode
	if billing == "" {
		billing = "PROVISIONED"
	}
	if billing != "PROVISIONED" && billing != "PAY_PER_REQUEST" {
		return nil, enumFailErr("billingMode", req.BillingMode, "PAY_PER_REQUEST, PROVISIONED")
	}
	rcu, wcu := 0, 0
	if billing == "PROVISIONED" {
		if req.ProvisionedThroughput == nil || req.ProvisionedThroughput.ReadCapacityUnits == nil || req.ProvisionedThroughput.WriteCapacityUnits == nil {
			return nil, verr("One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits are required when BillingMode is PROVISIONED")
		}
		rcu = int(*req.ProvisionedThroughput.ReadCapacityUnits)
		wcu = int(*req.ProvisionedThroughput.WriteCapacityUnits)
	}
	lsiAny := []any{}
	for _, raw := range req.LocalSecondaryIndexes {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			lsiAny = append(lsiAny, v)
		}
	}
	var gsiAny []any
	for _, raw := range req.GlobalSecondaryIndexes {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			gsiAny = append(gsiAny, v)
		}
	}
	if store.getTable(req.TableName) != nil {
		return nil, tableInUseErr(req.TableName)
	}
	t := &table{
		Name:      req.TableName,
		KeySchema: keys,
		AttrDefs:  adList,
		Billing:   billing,
		RCU:       rcu,
		WCU:       wcu,
		Items:     map[string]map[string]*av{},
		Created:   time.Now(),
		LSIAny:    lsiAny,
		GSIAny:    gsiAny,
		StreamRaw: req.StreamSpecification,
	}
	store.tables[req.TableName] = t
	return map[string]any{"TableDescription": t.describe("ACTIVE")}, nil
}

func hDescribeTable(body []byte) (any, *apiErr) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	return map[string]any{"Table": t.describe("ACTIVE")}, nil
}

func hDeleteTable(body []byte) (any, *apiErr) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	if req.TableName == "" {
		return nil, requiredNullErr("tableName")
	}
	t := store.getTable(req.TableName)
	if t == nil {
		return nil, tableNotFoundErr(req.TableName)
	}
	delete(store.tables, req.TableName)
	return map[string]any{"TableDescription": t.describe("DELETING")}, nil
}

func hListTables(body []byte) (any, *apiErr) {
	var req struct {
		ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
		Limit                   *int64 `json:"Limit"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, serErr()
	}
	limit := 100
	if req.Limit != nil {
		if *req.Limit < 1 || *req.Limit > 100 {
			return nil, verr("1 validation error detected: Value '" + strconv.FormatInt(*req.Limit, 10) + "' at 'limit' failed to satisfy constraint: Member must have value less than or equal to 100")
		}
		limit = int(*req.Limit)
	}
	names := []string{}
	for n := range store.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	start := 0
	if req.ExclusiveStartTableName != "" {
		start = sort.Search(len(names), func(i int) bool { return names[i] > req.ExclusiveStartTableName })
	}
	res := names[start:]
	trunc := false
	if len(res) > limit {
		res = res[:limit]
		trunc = true
	}
	tables := []any{}
	for _, n := range res {
		tables = append(tables, n)
	}
	resp := map[string]any{"TableNames": tables}
	if trunc && len(res) > 0 {
		resp["LastEvaluatedTableName"] = res[len(res)-1]
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Wire layer
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		b = []byte("{}")
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	w.Write(b)
}

func main() {
	port := ""
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		port = "8000"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		if target == "" {
			writeJSON(w, 400, map[string]any{"__type": "com.amazon.coral.service#UnknownOperationException"})
			return
		}
		action := target
		if idx := strings.IndexByte(target, '.'); idx >= 0 {
			action = target[idx+1:]
		}
		body, _ := io.ReadAll(r.Body)
		var resp any
		var aerr *apiErr
		store.mu.Lock()
		switch action {
		case "UpdateItem":
			resp, aerr = hUpdateItem(body)
		case "PutItem":
			resp, aerr = hPutItem(body)
		case "GetItem":
			resp, aerr = hGetItem(body)
		case "DeleteItem":
			resp, aerr = hDeleteItem(body)
		case "BatchWriteItem":
			resp, aerr = hBatchWriteItem(body)
		case "CreateTable":
			resp, aerr = hCreateTable(body)
		case "DescribeTable":
			resp, aerr = hDescribeTable(body)
		case "DeleteTable":
			resp, aerr = hDeleteTable(body)
		case "ListTables":
			resp, aerr = hListTables(body)
		default:
			aerr = &apiErr{Status: 400, Type: "com.amazon.coral.service#UnknownOperationException"}
		}
		store.mu.Unlock()
		if aerr != nil {
			out := map[string]any{}
			if aerr.Type != "" {
				out["__type"] = aerr.Type
			}
			if aerr.Msg != "" {
				out["message"] = aerr.Msg
			}
			for k, v := range aerr.Extra {
				out[k] = v
			}
			writeJSON(w, aerr.Status, out)
			return
		}
		if resp == nil {
			resp = map[string]any{}
		}
		writeJSON(w, 200, resp)
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		os.Exit(1)
	}
}