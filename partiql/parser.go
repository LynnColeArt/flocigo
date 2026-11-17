package partiql

import (
  "fmt"
  "math/big"
  "strconv"
  "strings"
)

// This file ports floci's DynamoDbPartiQLParser: the PartiQL statement AST
// and the recursive-descent parser for SELECT / INSERT / UPDATE / DELETE /
// CREATE TABLE / DROP TABLE plus condition and value expressions.

// Stmt is a parsed PartiQL statement.
type Stmt interface{ stmt() }

type SelectStmt struct {
  Table   string
  Alias   string
  Star    bool
  Project []ProjItem
  Where   Expr
}

type ProjItem struct {
  Path  *PathExpr
  Alias string
}

type InsertStmt struct {
  Table     string
  Docs      []*MapLit
  Returning *Returning
}

type MapLit struct {
  Fields []MapField
}

type MapField struct {
  Key string
  Val Expr
}

type UpdateStmt struct {
  Table     string
  Sets      []SetAction
  Removes   []*PathExpr
  Where     Expr
  Returning *Returning
}

type SetAction struct {
  Path  *PathExpr
  Value Expr
}

type DeleteStmt struct {
  Table     string
  Where     Expr
  Returning *Returning
}

type CreateTableStmt struct {
  Table string
  Attrs []AttrDef
  Keys  []KeyDef
}

type AttrDef struct{ Name, Typ string }

type KeyDef struct{ Name, Role string } // Role: HASH | RANGE

type DropTableStmt struct {
  Table string
}

// Returning mirrors the DynamoDB PartiQL RETURNING clause.
type Returning struct {
  Scope   string // ALL | MODIFIED
  Version string // OLD | NEW
}

func (*SelectStmt) stmt()      {}
func (*InsertStmt) stmt()      {}
func (*UpdateStmt) stmt()      {}
func (*DeleteStmt) stmt()      {}
func (*CreateTableStmt) stmt() {}
func (*DropTableStmt) stmt()   {}

// Expr is an expression node. One tree serves condition contexts (WHERE) and
// value contexts (SET, INSERT documents, function arguments).
type Expr interface{ expr() }

type LitExpr struct {
  Val     *AttrValue
  Missing bool // the MISSING keyword
}

type ParamExpr struct {
  Index int  // positional parameter (0-based)
  Named bool // :name instead of positional
  Name  string
}

type PathExpr struct {
  Root  string
  Steps []PathStep
}

type PathStep struct {
  Key     string // map key when !IsIdx
  Index   int    // literal list index when IsIdx
  IsIdx   bool
  IdxExpr Expr // non-literal index expression
}

type UnaryExpr struct {
  Op string // "-" or "NOT"
  X  Expr
}

type BinExpr struct {
  Op   string // AND OR = <> < <= > >= + - * / % ||
  L, R Expr
}

type BetweenExpr struct {
  X, Lo, Hi Expr
  Neg       bool
}

type InExpr struct {
  X    Expr
  List []Expr
  Neg  bool
}

type IsExpr struct {
  X    Expr
  Kind string // NULL | MISSING
  Neg  bool
}

type FuncExpr struct {
  Name string // lower-case
  Args []Expr
}

type MapLitExpr struct{ Lit *MapLit }
type ListLitExpr struct{ Items []Expr }

func (*LitExpr) expr()     {}
func (*ParamExpr) expr()   {}
func (*PathExpr) expr()    {}
func (*UnaryExpr) expr()   {}
func (*BinExpr) expr()     {}
func (*BetweenExpr) expr() {}
func (*InExpr) expr()      {}
func (*IsExpr) expr()      {}
func (*FuncExpr) expr()    {}
func (*MapLitExpr) expr()  {}
func (*ListLitExpr) expr() {}

// Parse parses a PartiQL statement into its AST.
func Parse(sql string) (Stmt, error) {
  return parseStatement(sql)
}

func parseStatement(src string) (Stmt, error) {
  toks, err := tokenize(src)
  if err != nil {
    return nil, err
  }
  p := &parser{toks: toks}
  stmt, err := p.parseStmt()
  if err != nil {
    return nil, err
  }
  if p.cur().Kind != tokEOF {
    return nil, p.fail("unexpected trailing input")
  }
  return stmt, nil
}

type parser struct {
  toks        []token
  i           int
  nextUnnamed int
}

func (p *parser) cur() token  { return p.toks[p.i] }
func (p *parser) peek() token { if p.i+1 < len(p.toks) { return p.toks[p.i+1] }; return p.toks[len(p.toks)-1] }
func (p *parser) next() token {
  t := p.toks[p.i]
  if p.i < len(p.toks)-1 {
    p.i++
  }
  return t
}

func (p *parser) fail(msg string) error {
  t := p.cur()
  return &Error{Code: CodeValidation, Msg: fmt.Sprintf("syntax error: %s (near %q, offset %d)", msg, t.Text, t.Pos)}
}

func (p *parser) expectKeyword(w string) error {
  if !p.cur().isKeyword(w) {
    return p.fail("expected " + w)
  }
  p.next()
  return nil
}

func (p *parser) expectSymbol(w string) error {
  if !p.cur().isSymbol(w) {
    return p.fail("expected " + w)
  }
  p.next()
  return nil
}

func (p *parser) parseStmt() (Stmt, error) {
  t := p.cur()
  switch {
  case t.isKeyword("SELECT"):
    return p.parseSelect()
  case t.isKeyword("INSERT"):
    return p.parseInsert()
  case t.isKeyword("UPDATE"):
    return p.parseUpdate()
  case t.isKeyword("DELETE"):
    return p.parseDelete()
  case t.isKeyword("CREATE"):
    return p.parseCreate()
  case t.isKeyword("DROP"):
    return p.parseDrop()
  }
  if t.Kind == tokEOF {
    return nil, p.fail("empty statement")
  }
  return nil, p.fail("expected a statement keyword")
}

func (p *parser) parseIdent(what string) (string, error) {
  t := p.cur()
  if t.Kind == tokIdent {
    p.next()
    return t.Text, nil
  }
  return "", p.fail("expected " + what)
}

func (p *parser) parseSelect() (*SelectStmt, error) {
  if err := p.expectKeyword("SELECT"); err != nil {
    return nil, err
  }
  if p.cur().isKeyword("DISTINCT") {
    return nil, p.fail("DISTINCT is not supported")
  }
  sel := &SelectStmt{}
  if p.cur().isSymbol("*") {
    sel.Star = true
    p.next()
  } else {
    for {
      item, err := p.parseProjItem()
      if err != nil {
        return nil, err
      }
      sel.Project = append(sel.Project, item)
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
  }
  if err := p.expectKeyword("FROM"); err != nil {
    return nil, err
  }
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  sel.Table = tbl
  if p.cur().isKeyword("AS") {
    p.next()
    alias, err := p.parseIdent("table alias")
    if err != nil {
      return nil, err
    }
    sel.Alias = alias
  }
  if p.cur().isKeyword("WHERE") {
    p.next()
    w, err := p.parseExpr()
    if err != nil {
      return nil, err
    }
    sel.Where = w
  }
  return sel, nil
}

func (p *parser) parseProjItem() (ProjItem, error) {
  px, err := p.parsePathOnly()
  if err != nil {
    return ProjItem{}, err
  }
  item := ProjItem{Path: px}
  if p.cur().isKeyword("AS") {
    p.next()
    alias, err := p.parseIdent("projection alias")
    if err != nil {
      return ProjItem{}, err
    }
    item.Alias = alias
  }
  return item, nil
}

func (p *parser) parsePathOnly() (*PathExpr, error) {
  root, err := p.parseIdent("attribute path")
  if err != nil {
    return nil, err
  }
  px := &PathExpr{Root: root}
  if err := p.parsePathSteps(px); err != nil {
    return nil, err
  }
  return px, nil
}

func (p *parser) parsePathSteps(px *PathExpr) error {
  for {
    t := p.cur()
    if t.isSymbol(".") {
      p.next()
      kt := p.cur()
      switch kt.Kind {
      case tokIdent, tokKeyword, tokString:
        p.next()
        px.Steps = append(px.Steps, PathStep{Key: kt.Text})
      default:
        return p.fail("expected attribute name after '.'")
      }
      continue
    }
    if t.isSymbol("[") {
      p.next()
      idx, err := p.parseExpr()
      if err != nil {
        return err
      }
      if err := p.expectSymbol("]"); err != nil {
        return err
      }
      st := PathStep{IsIdx: true, IdxExpr: idx}
      if lit, ok := idx.(*LitExpr); ok && lit.Val != nil && lit.Val.Type == "N" {
        if n, ok := integralIndex(lit.Val.N); ok {
          st.Index = n
          st.IdxExpr = nil
        }
      }
      px.Steps = append(px.Steps, st)
      continue
    }
    return nil
  }
}

func (p *parser) parseInsert() (*InsertStmt, error) {
  if err := p.expectKeyword("INSERT"); err != nil {
    return nil, err
  }
  if err := p.expectKeyword("INTO"); err != nil {
    return nil, err
  }
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  ins := &InsertStmt{Table: tbl}
  if !p.cur().isKeyword("VALUE") && !p.cur().isKeyword("VALUES") {
    return nil, p.fail("expected VALUE or VALUES")
  }
  p.next()
  for {
    lit, err := p.parseMapLit()
    if err != nil {
      return nil, err
    }
    ins.Docs = append(ins.Docs, lit)
    if p.cur().isSymbol(",") {
      p.next()
      continue
    }
    break
  }
  ret, err := p.parseOptionalReturning()
  if err != nil {
    return nil, err
  }
  ins.Returning = ret
  return ins, nil
}

func (p *parser) parseMapLit() (*MapLit, error) {
  if err := p.expectSymbol("{"); err != nil {
    return nil, err
  }
  lit := &MapLit{}
  if !p.cur().isSymbol("}") {
    for {
      kt := p.cur()
      var key string
      switch kt.Kind {
      case tokString, tokIdent, tokKeyword:
        key = kt.Text
        p.next()
      default:
        return nil, p.fail("expected attribute name in document literal")
      }
      if err := p.expectSymbol(":"); err != nil {
        return nil, err
      }
      v, err := p.parseExpr()
      if err != nil {
        return nil, err
      }
      lit.Fields = append(lit.Fields, MapField{Key: key, Val: v})
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
  }
  if err := p.expectSymbol("}"); err != nil {
    return nil, err
  }
  return lit, nil
}

func (p *parser) parseUpdate() (*UpdateStmt, error) {
  if err := p.expectKeyword("UPDATE"); err != nil {
    return nil, err
  }
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  u := &UpdateStmt{Table: tbl}
  if !p.cur().isKeyword("SET") && !p.cur().isKeyword("REMOVE") {
    return nil, p.fail("expected SET or REMOVE")
  }
  if p.cur().isKeyword("SET") {
    p.next()
    for {
      px, err := p.parsePathOnly()
      if err != nil {
        return nil, err
      }
      if err := p.expectSymbol("="); err != nil {
        return nil, err
      }
      val, err := p.parseExpr()
      if err != nil {
        return nil, err
      }
      u.Sets = append(u.Sets, SetAction{Path: px, Value: val})
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
  }
  if p.cur().isKeyword("REMOVE") {
    p.next()
    for {
      px, err := p.parsePathOnly()
      if err != nil {
        return nil, err
      }
      u.Removes = append(u.Removes, px)
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
  }
  if p.cur().isKeyword("WHERE") {
    p.next()
    w, err := p.parseExpr()
    if err != nil {
      return nil, err
    }
    u.Where = w
  }
  ret, err := p.parseOptionalReturning()
  if err != nil {
    return nil, err
  }
  u.Returning = ret
  return u, nil
}

func (p *parser) parseDelete() (*DeleteStmt, error) {
  if err := p.expectKeyword("DELETE"); err != nil {
    return nil, err
  }
  if err := p.expectKeyword("FROM"); err != nil {
    return nil, err
  }
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  d := &DeleteStmt{Table: tbl}
  if p.cur().isKeyword("WHERE") {
    p.next()
    w, err := p.parseExpr()
    if err != nil {
      return nil, err
    }
    d.Where = w
  }
  ret, err := p.parseOptionalReturning()
  if err != nil {
    return nil, err
  }
  d.Returning = ret
  return d, nil
}

func (p *parser) parseOptionalReturning() (*Returning, error) {
  if !p.cur().isKeyword("RETURNING") {
    return nil, nil
  }
  p.next()
  ret := &Returning{Scope: "ALL", Version: "NEW"}
  if p.cur().isKeyword("ALL") || p.cur().isKeyword("MODIFIED") {
    ret.Scope = p.cur().Text
    p.next()
  }
  if p.cur().isKeyword("OLD") || p.cur().isKeyword("NEW") {
    ret.Version = p.cur().Text
    p.next()
  }
  if p.cur().isKeyword("ITEMS") {
    p.next()
  }
  if p.cur().isSymbol("*") {
    p.next()
  }
  return ret, nil
}

func (p *parser) parseCreate() (Stmt, error) {
  if err := p.expectKeyword("CREATE"); err != nil {
    return nil, err
  }
  if !p.cur().isKeyword("TABLE") {
    return nil, p.fail("expected TABLE")
  }
  p.next()
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  ct := &CreateTableStmt{Table: tbl}
  switch {
  case p.cur().isSymbol("("):
    p.next()
    for {
      if p.cur().isKeyword("PRIMARY") {
        if err := p.parsePrimaryKey(ct); err != nil {
          return nil, err
        }
        break
      }
      name, err := p.parseIdent("attribute name")
      if err != nil {
        return nil, err
      }
      typ, err := p.parseTypeName()
      if err != nil {
        return nil, err
      }
      ct.Attrs = append(ct.Attrs, AttrDef{Name: name, Typ: typ})
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
    if err := p.expectSymbol(")"); err != nil {
      return nil, err
    }
    if p.cur().isKeyword("PRIMARY") {
      if err := p.parsePrimaryKey(ct); err != nil {
        return nil, err
      }
    }
  case p.cur().isKeyword("PRIMARY"):
    if err := p.parsePrimaryKey(ct); err != nil {
      return nil, err
    }
  default:
    return nil, p.fail("expected attribute list or PRIMARY KEY")
  }
  if len(ct.Keys) == 0 {
    return nil, p.fail("CREATE TABLE requires PRIMARY KEY")
  }
  return ct, nil
}

func (p *parser) parsePrimaryKey(ct *CreateTableStmt) error {
  if err := p.expectKeyword("PRIMARY"); err != nil {
    return err
  }
  if err := p.expectKeyword("KEY"); err != nil {
    return err
  }
  if err := p.expectSymbol("("); err != nil {
    return err
  }
  for {
    name, err := p.parseIdent("key attribute name")
    if err != nil {
      return err
    }
    if !p.cur().isKeyword("HASH") && !p.cur().isKeyword("RANGE") {
      return p.fail("expected HASH or RANGE")
    }
    role := p.cur().Text
    p.next()
    ct.Keys = append(ct.Keys, KeyDef{Name: name, Role: role})
    if p.cur().isSymbol(",") {
      p.next()
      continue
    }
    break
  }
  return p.expectSymbol(")")
}

func (p *parser) parseTypeName() (string, error) {
  t := p.cur()
  switch t.Kind {
  case tokIdent, tokString, tokKeyword:
    p.next()
    return strings.ToLower(t.Text), nil
  }
  return "", p.fail("expected attribute type")
}

func (p *parser) parseDrop() (Stmt, error) {
  if err := p.expectKeyword("DROP"); err != nil {
    return nil, err
  }
  if !p.cur().isKeyword("TABLE") {
    return nil, p.fail("expected TABLE")
  }
  p.next()
  tbl, err := p.parseIdent("table name")
  if err != nil {
    return nil, err
  }
  return &DropTableStmt{Table: tbl}, nil
}

func (p *parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
  l, err := p.parseAnd()
  if err != nil {
    return nil, err
  }
  for p.cur().isKeyword("OR") {
    p.next()
    r, err := p.parseAnd()
    if err != nil {
      return nil, err
    }
    l = &BinExpr{Op: "OR", L: l, R: r}
  }
  return l, nil
}

func (p *parser) parseAnd() (Expr, error) {
  l, err := p.parseNot()
  if err != nil {
    return nil, err
  }
  for p.cur().isKeyword("AND") {
    p.next()
    r, err := p.parseNot()
    if err != nil {
      return nil, err
    }
    l = &BinExpr{Op: "AND", L: l, R: r}
  }
  return l, nil
}

func (p *parser) parseNot() (Expr, error) {
  if p.cur().isKeyword("NOT") {
    p.next()
    x, err := p.parseNot()
    if err != nil {
      return nil, err
    }
    return &UnaryExpr{Op: "NOT", X: x}, nil
  }
  return p.parsePredicate()
}

func (p *parser) parsePredicate() (Expr, error) {
  x, err := p.parseAdditive()
  if err != nil {
    return nil, err
  }
  t := p.cur()
  switch {
  case t.isKeyword("IS"):
    p.next()
    neg := false
    if p.cur().isKeyword("NOT") {
      p.next()
      neg = true
    }
    switch {
    case p.cur().isKeyword("NULL"):
      p.next()
      return &IsExpr{X: x, Kind: "NULL", Neg: neg}, nil
    case p.cur().isKeyword("MISSING"):
      p.next()
      return &IsExpr{X: x, Kind: "MISSING", Neg: neg}, nil
    }
    return nil, p.fail("expected NULL or MISSING after IS")
  case t.isKeyword("BETWEEN"):
    p.next()
    return p.parseBetweenTail(x, false)
  case t.isKeyword("IN"):
    return p.parseIn(x, false)
  case t.isKeyword("NOT"):
    if !p.peek().isKeyword("BETWEEN") && !p.peek().isKeyword("IN") {
      return x, nil
    }
    p.next()
    if p.cur().isKeyword("BETWEEN") {
      p.next()
      return p.parseBetweenTail(x, true)
    }
    p.next() // IN
    return p.parseIn(x, true)
  }
  if t.Kind == tokSymbol {
    switch t.Text {
    case "=", "<>", "!=", "<", "<=", ">", ">=":
      p.next()
      op := t.Text
      if op == "!=" {
        op = "<>"
      }
      r, err := p.parseAdditive()
      if err != nil {
        return nil, err
      }
      return &BinExpr{Op: op, L: x, R: r}, nil
    }
  }
  return x, nil
}

func (p *parser) parseBetweenTail(x Expr, neg bool) (Expr, error) {
  lo, err := p.parseAdditive()
  if err != nil {
    return nil, err
  }
  if err := p.expectKeyword("AND"); err != nil {
    return nil, err
  }
  hi, err := p.parseAdditive()
  if err != nil {
    return nil, err
  }
  return &BetweenExpr{X: x, Lo: lo, Hi: hi, Neg: neg}, nil
}

func (p *parser) parseIn(x Expr, neg bool) (Expr, error) {
  if err := p.expectSymbol("("); err != nil {
    return nil, err
  }
  var list []Expr
  for {
    e, err := p.parseExpr()
    if err != nil {
      return nil, err
    }
    list = append(list, e)
    if p.cur().isSymbol(",") {
      p.next()
      continue
    }
    break
  }
  if err := p.expectSymbol(")"); err != nil {
    return nil, err
  }
  return &InExpr{X: x, List: list, Neg: neg}, nil
}

func (p *parser) parseAdditive() (Expr, error) {
  l, err := p.parseMultiplicative()
  if err != nil {
    return nil, err
  }
  for {
    t := p.cur()
    if t.Kind == tokSymbol && (t.Text == "+" || t.Text == "-" || t.Text == "||") {
      p.next()
      r, err := p.parseMultiplicative()
      if err != nil {
        return nil, err
      }
      l = &BinExpr{Op: t.Text, L: l, R: r}
      continue
    }
    return l, nil
  }
}

func (p *parser) parseMultiplicative() (Expr, error) {
  l, err := p.parseUnary()
  if err != nil {
    return nil, err
  }
  for {
    t := p.cur()
    if t.Kind == tokSymbol && (t.Text == "*" || t.Text == "/" || t.Text == "%") {
      p.next()
      r, err := p.parseUnary()
      if err != nil {
        return nil, err
      }
      l = &BinExpr{Op: t.Text, L: l, R: r}
      continue
    }
    return l, nil
  }
}

func (p *parser) parseUnary() (Expr, error) {
  t := p.cur()
  if t.Kind == tokSymbol && (t.Text == "-" || t.Text == "+") {
    p.next()
    x, err := p.parseUnary()
    if err != nil {
      return nil, err
    }
    if t.Text == "+" {
      return x, nil
    }
    if lit, ok := x.(*LitExpr); ok && lit.Val != nil && lit.Val.Type == "N" {
      return &LitExpr{Val: &AttrValue{Type: "N", N: negateNumber(lit.Val.N)}}, nil
    }
    return &UnaryExpr{Op: "-", X: x}, nil
  }
  return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
  t := p.cur()
  switch t.Kind {
  case tokNumber:
    p.next()
    return &LitExpr{Val: NVal(t.Text)}, nil
  case tokString:
    p.next()
    return &LitExpr{Val: SVal(t.Text)}, nil
  case tokParam:
    p.next()
    return p.paramExpr(t), nil
  case tokKeyword:
    switch t.Text {
    case "TRUE":
      p.next()
      return &LitExpr{Val: BoolVal(true)}, nil
    case "FALSE":
      p.next()
      return &LitExpr{Val: BoolVal(false)}, nil
    case "NULL":
      p.next()
      return &LitExpr{Val: NullVal()}, nil
    case "MISSING":
      p.next()
      return &LitExpr{Missing: true}, nil
    }
    return nil, p.fail("unexpected keyword in expression")
  case tokSymbol:
    switch t.Text {
    case "(":
      p.next()
      x, err := p.parseExpr()
      if err != nil {
        return nil, err
      }
      if err := p.expectSymbol(")"); err != nil {
        return nil, err
      }
      return x, nil
    case "{":
      lit, err := p.parseMapLit()
      if err != nil {
        return nil, err
      }
      return &MapLitExpr{Lit: lit}, nil
    case "[":
      p.next()
      var items []Expr
      if !p.cur().isSymbol("]") {
        for {
          e, err := p.parseExpr()
          if err != nil {
            return nil, err
          }
          items = append(items, e)
          if p.cur().isSymbol(",") {
            p.next()
            continue
          }
          break
        }
      }
      if err := p.expectSymbol("]"); err != nil {
        return nil, err
      }
      return &ListLitExpr{Items: items}, nil
    }
    return nil, p.fail("unexpected symbol in expression")
  case tokIdent:
    if p.peek().isSymbol("(") {
      name := strings.ToLower(t.Text)
      p.next()
      p.next()
      args, err := p.parseArgs()
      if err != nil {
        return nil, err
      }
      return &FuncExpr{Name: name, Args: args}, nil
    }
    return p.parsePathOnly()
  }
  return nil, p.fail("unexpected token in expression")
}

func (p *parser) parseArgs() ([]Expr, error) {
  if err := p.expectSymbol("("); err != nil {
    return nil, err
  }
  var args []Expr
  if !p.cur().isSymbol(")") {
    for {
      e, err := p.parseExpr()
      if err != nil {
        return nil, err
      }
      args = append(args, e)
      if p.cur().isSymbol(",") {
        p.next()
        continue
      }
      break
    }
  }
  if err := p.expectSymbol(")"); err != nil {
    return nil, err
  }
  return args, nil
}

func (p *parser) paramExpr(t token) *ParamExpr {
  if strings.HasPrefix(t.Text, ":") {
    return &ParamExpr{Named: true, Name: t.Text[1:]}
  }
  if t.Text == "?" {
    idx := p.nextUnnamed
    p.nextUnnamed++
    return &ParamExpr{Index: idx}
  }
  n, _ := strconv.Atoi(t.Text[1:])
  return &ParamExpr{Index: n}
}

func integralIndex(s string) (int, bool) {
  r, ok := parseRat(s)
  if !ok {
    return 0, false
  }
  q := new(big.Int).Quo(r.Num(), r.Denom())
  exact := new(big.Rat).SetInt(q)
  if r.Cmp(exact) != 0 || r.Sign() < 0 {
    return 0, false
  }
  if !q.IsInt64() {
    return 0, false
  }
  return int(q.Int64()), true
}
