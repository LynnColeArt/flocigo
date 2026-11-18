package partiql

// KeyClause is one comparison of a key condition (DynamoDbConditionKeys.java).
type KeyClause struct {
	Op   string // EQ, LT, LE, GT, GE, BETWEEN, BEGINS_WITH
	Attr string
	Refs []string
}

// KeyCondition is the parsed KeyConditionExpression: a partition-key
// equality plus an optional sort-key clause joined by a single AND.
type KeyCondition struct {
	Partition *KeyClause
	Sort      *KeyClause
	Attrs     []string
	Refs      []string
}

var sortKeyOps = map[string]bool{
	"EQ": true, "LT": true, "LE": true, "GT": true, "GE": true,
	"BETWEEN": true, "BEGINS_WITH": true,
}

var keyCmpSym = map[string]string{"=": "EQ", "<": "LT", "<=": "LE", ">": "GT", ">=": "GE"}

func keyCondUnsupported() *ValidationError {
	return verr(CodeValidation, "Query key condition not supported")
}

func (p *parser) parseKeyCondition() (*KeyCondition, error) {
	c1, err := p.parseKeyClause()
	if err != nil {
		return nil, err
	}
	var c2 *KeyClause
	if p.isKw("and") {
		p.eat()
		c2, err = p.parseKeyClause()
		if err != nil {
			return nil, err
		}
	}
	if t := p.cur(); t.Kind != tkEOF {
		return nil, keyCondUnsupported()
	}
	if c1.Op != "EQ" {
		return nil, keyCondUnsupported()
	}
	if c2 != nil && !sortKeyOps[c2.Op] {
		return nil, keyCondUnsupported()
	}
	kc := &KeyCondition{Partition: c1, Sort: c2, Attrs: []string{c1.Attr}}
	if c2 != nil {
		if c2.Attr == c1.Attr {
			return nil, keyCondUnsupported()
		}
		kc.Attrs = append(kc.Attrs, c2.Attr)
	}
	kc.Refs = append(kc.Refs, c1.Refs...)
	if c2 != nil {
		kc.Refs = append(kc.Refs, c2.Refs...)
	}
	return kc, nil
}

func (p *parser) parseKeyClause() (*KeyClause, error) {
	t := p.cur()
	if t.Kind == tkWord && foldWord(t) == "begins_with" && p.peekAt(1).Kind == tkSym && p.peekAt(1).Text == "(" {
		return p.parseKeyBeginsWith()
	}
	if t.Kind != tkWord && t.Kind != tkNameRef {
		return nil, keyCondUnsupported()
	}
	pth, err := p.parsePathFrom(t)
	if err != nil {
		return nil, err
	}
	if len(pth.Elems) != 1 || pth.Elems[0].IsIdx {
		return nil, keyCondUnsupported()
	}
	nt := p.cur()
	if nt.Kind == tkSym {
		if op, ok := keyCmpSym[nt.Text]; ok {
			p.eat()
			rt := p.cur()
			if rt.Kind != tkValueRef {
				return nil, keyCondUnsupported()
			}
			p.eat()
			p.usedV[rt.Text] = struct{}{}
			return &KeyClause{Op: op, Attr: pth.Elems[0].Name, Refs: []string{rt.Text}}, nil
		}
	}
	if p.isKw("between") {
		p.eat()
		lo := p.cur()
		if lo.Kind != tkValueRef {
			return nil, keyCondUnsupported()
		}
		p.eat()
		p.usedV[lo.Text] = struct{}{}
		if !p.isKw("and") {
			return nil, keyCondUnsupported()
		}
		p.eat()
		hi := p.cur()
		if hi.Kind != tkValueRef {
			return nil, keyCondUnsupported()
		}
		p.eat()
		p.usedV[hi.Text] = struct{}{}
		return &KeyClause{Op: "BETWEEN", Attr: pth.Elems[0].Name, Refs: []string{lo.Text, hi.Text}}, nil
	}
	return nil, keyCondUnsupported()
}

func (p *parser) parseKeyBeginsWith() (*KeyClause, error) {
	p.eat() // begins_with
	p.eat() // '('
	at := p.cur()
	if at.Kind != tkWord && at.Kind != tkNameRef {
		return nil, keyCondUnsupported()
	}
	pth, err := p.parsePathFrom(at)
	if err != nil {
		return nil, err
	}
	if len(pth.Elems) != 1 || pth.Elems[0].IsIdx {
		return nil, keyCondUnsupported()
	}
	if err := p.expectSym(","); err != nil {
		return nil, err
	}
	rt := p.cur()
	if rt.Kind != tkValueRef {
		return nil, keyCondUnsupported()
	}
	p.eat()
	p.usedV[rt.Text] = struct{}{}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	return &KeyClause{Op: "BEGINS_WITH", Attr: pth.Elems[0].Name, Refs: []string{rt.Text}}, nil
}

// Validate checks the parsed key condition against the table's key schema
// (partition key pk, optional sort key sk), producing the exact query
// validation failures of the Java DynamoDbConditionKeys logic.
func (kc *KeyCondition) Validate(pk, sk string) error {
	if kc.Partition.Attr != pk {
		return verr(CodeValidation, "Query condition missed key schema element: %s", pk)
	}
	if kc.Sort != nil {
		if sk == "" {
			return keyCondUnsupported()
		}
		if kc.Sort.Attr != sk {
			return verr(CodeValidation, "Query condition missed key schema element: %s", sk)
		}
	}
	return nil
}

// CheckValues ensures every :value reference of the key condition is
// provided.
func (kc *KeyCondition) CheckValues(values map[string]AV) error {
	for _, r := range kc.Refs {
		if _, ok := values[r]; !ok {
			return valueNotDefinedErr(r)
		}
	}
	return nil
}
