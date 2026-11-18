package partiql

import (
	"sort"
	"strconv"
)

// parser drives the shared grammar of all expression kinds.
type parser struct {
	src   string
	toks  []Token
	pos   int
	kind  ExprKind
	names map[string]string
	usedN map[string]struct{}
	usedV map[string]struct{}
}

func newParser(kind ExprKind, src string, names map[string]string) (*parser, error) {
	toks, bad := lex(src)
	if bad != nil {
		return nil, syntaxErr(kind, bad.Text, nearText(src, *bad))
	}
	return &parser{
		src:   src,
		toks:  toks,
		kind:  kind,
		names: names,
		usedN: map[string]struct{}{},
		usedV: map[string]struct{}{},
	}, nil
}

func (p *parser) cur() Token { return p.toks[p.pos] }

func (p *parser) peekAt(i int) Token {
	if p.pos+i < len(p.toks) {
		return p.toks[p.pos+i]
	}
	return p.toks[len(p.toks)-1]
}

func (p *parser) eat() Token {
	t := p.cur()
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) isKw(w string) bool {
	t := p.cur()
	return t.Kind == tkWord && foldWord(t) == w
}

func (p *parser) atSym(s string) bool {
	t := p.cur()
	return t.Kind == tkSym && t.Text == s
}

func (p *parser) expectSym(s string) error {
	if !p.atSym(s) {
		return p.failTok(p.cur())
	}
	p.eat()
	return nil
}

func (p *parser) failTok(t Token) error {
	return syntaxErr(p.kind, reportText(t), nearText(p.src, t))
}

func (p *parser) usedList() (names []string, vals []string) {
	for k := range p.usedN {
		names = append(names, k)
	}
	for k := range p.usedV {
		vals = append(vals, k)
	}
	sort.Strings(names)
	sort.Strings(vals)
	return names, vals
}

// identifier validates one path element. Structural keywords are grammar
// tokens (syntax error when used as a name) and reserved words may not be
// used unquoted, both exactly like the Java parsers.
func (p *parser) identifier(t Token) (string, error) {
	switch t.Kind {
	case tkNameRef:
		p.usedN[t.Text] = struct{}{}
		name, ok := p.names[t.Text]
		if !ok {
			return "", nameNotDefinedErr(t.Text)
		}
		return name, nil
	case tkWord:
		w := foldWord(t)
		if actionKw[w] || condKw[w] {
			return "", p.failTok(t)
		}
		if isReserved(w) {
			return "", reservedKeywordErr(t.Text)
		}
		return t.Text, nil
	default:
		return "", p.failTok(t)
	}
}

// parsePathFrom parses an attribute path whose first element is tok (not
// yet consumed): element ('.' element | '[' index ']')*.
func (p *parser) parsePathFrom(first Token) (Path, error) {
	name, err := p.identifier(first)
	if err != nil {
		return Path{}, err
	}
	p.eat()
	elems := []PathElem{{Name: name, Orig: first.Text}}
	for {
		if p.atSym(".") {
			p.eat()
			nt := p.cur()
			name2, err := p.identifier(nt)
			if err != nil {
				return Path{}, err
			}
			p.eat()
			elems = append(elems, PathElem{Name: name2, Orig: nt.Text})
			continue
		}
		if p.atSym("[") {
			p.eat()
			it := p.cur()
			if it.Kind != tkInt {
				return Path{}, p.failTok(it)
			}
			idx, perr := strconv.ParseUint(it.Text, 10, 30)
			if perr != nil {
				return Path{}, p.failTok(it)
			}
			p.eat()
			if err := p.expectSym("]"); err != nil {
				return Path{}, err
			}
			elems = append(elems, PathElem{Index: int(idx), IsIdx: true, Orig: "[" + it.Text + "]"})
			continue
		}
		break
	}
	return Path{Elems: elems}, nil
}

// Parsed bundles the parsed AST of an expression clause together with the
// references it consumed (for the unused-name/value checks).
type Parsed struct {
	Kind  ExprKind
	Cond  *Condition
	Upd   *Update
	Key   *KeyCondition
	Proj  []Path
	UsedN []string
	UsedV []string
}

// Parse parses the expression attached to the given clause. names is the
// ExpressionAttributeNames map (possibly nil).
func Parse(kind ExprKind, src string, names map[string]string) (*Parsed, error) {
	p, err := newParser(kind, src, names)
	if err != nil {
		return nil, err
	}
	out := &Parsed{Kind: kind}
	switch kind {
	case KindUpdate:
		u, err := p.parseUpdate()
		if err != nil {
			return nil, err
		}
		out.Upd = u
	case KindKeyCondition:
		kc, err := p.parseKeyCondition()
		if err != nil {
			return nil, err
		}
		out.Key = kc
	case KindProjection:
		proj, err := p.parseProjection()
		if err != nil {
			return nil, err
		}
		out.Proj = proj
	default:
		root, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		out.Cond = &Condition{Root: root}
	}
	out.UsedN, out.UsedV = p.usedList()
	return out, nil
}

// EvalCondition evaluates a parsed condition / filter expression against an
// item.
func (p *Parsed) EvalCondition(item AV, values map[string]AV) (bool, error) {
	if p.Cond == nil {
		return false, verr(CodeValidation, "expression is not a condition")
	}
	return evalCond(p.Cond.Root, &evalCtx{item: item, values: values, kind: p.Kind})
}

// Apply executes a parsed update expression against an item.
func (p *Parsed) Apply(item AV, values map[string]AV) error {
	if p.Upd == nil {
		return verr(CodeValidation, "expression is not an update")
	}
	return p.Upd.Apply(item, values, p.Kind)
}

// EvalCondition parses and evaluates a condition expression in one step.
func EvalCondition(kind ExprKind, src string, item AV, names map[string]string, values map[string]AV) (bool, error) {
	pr, err := Parse(kind, src, names)
	if err != nil {
		return false, err
	}
	return pr.EvalCondition(item, values)
}

// ApplyUpdate parses and applies an update expression in one step.
func ApplyUpdate(src string, item AV, names map[string]string, values map[string]AV) error {
	pr, err := Parse(KindUpdate, src, names)
	if err != nil {
		return err
	}
	return pr.Apply(item, values)
}

// CheckUnusedValues reports the exact unused-values ValidationException
// when the caller provided values the expressions do not use.
func CheckUnusedValues(values map[string]AV, used []string) error {
	set := map[string]bool{}
	for _, u := range used {
		set[u] = true
	}
	var extra []string
	for k := range values {
		if !set[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	sort.Strings(extra)
	return unusedValuesErr(extra)
}

// CheckUnusedNames likewise for ExpressionAttributeNames.
func CheckUnusedNames(names map[string]string, used []string) error {
	set := map[string]bool{}
	for _, u := range used {
		set[u] = true
	}
	var extra []string
	for k := range names {
		if !set[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	sort.Strings(extra)
	return unusedNamesErr(extra)
}

func (p *parser) parseProjection() ([]Path, error) {
	var out []Path
	for {
		t := p.cur()
		pth, err := p.parsePathFrom(t)
		if err != nil {
			return nil, err
		}
		out = append(out, pth)
		if p.atSym(",") {
			p.eat()
			continue
		}
		break
	}
	if t := p.cur(); t.Kind != tkEOF {
		return nil, p.failTok(t)
	}
	root := newOverlapNode()
	for _, pth := range out {
		if _, dup, ov := root.insert(pth.Elems, "projection"); dup || ov {
			return nil, overlapErr(p.kind)
		}
	}
	return out, nil
}

// actionKw and condKw gate identifiers that are treated as keywords when
// parsed in attribute-name position. Names that overlap the update-
// expression grammar (SET/REMOVE/ADD/DELETE) or the condition-expression
// grammar (AND/OR/NOT/IN/BETWEEN/IS/EXISTS/...) cannot be unquoted
// identifiers, matching the AWS parser's token rule.
var actionKw = map[string]bool{
	updSet: true, updRemove: true, updAdd: true, updDelete: true,
}
var condKw = map[string]bool{
	"and": true, "or": true, "not": true, "in": true, "between": true,
	"is": true, "exists": true, "null": true, "begin_with": true,
	"begins_with": true, "contains": true, "size": true, "attribute_type": true,
}
