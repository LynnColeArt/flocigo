package ddb

import (
	"fmt"
	"math/big"
	"strings"
)

const invalidUpdatePathMsg = "The document path provided in the update expression is invalid for update"

func invalidPathErrUpd() *APIError {
	return validationErr(invalidUpdatePathMsg)
}

type setAction struct {
	path pathNode
	src  valueNode
}
type removeAction struct{ path pathNode }
type addAction struct {
	path    pathNode
	operand valueNode
}
type deleteAction struct {
	path    pathNode
	operand valueNode
}

// updateProgram is a compiled UpdateExpression.
type updateProgram struct {
	sets    []setAction
	removes []removeAction
	adds    []addAction
	deletes []deleteAction
}

func compileUpdate(expr string, names map[string]*string, values map[string]*AttributeValue) (*updateProgram, *APIError) {
	if strings.TrimSpace(expr) == "" {
		return nil, validationErr("An update expression must not be empty")
	}
	toks, err := tokenize("UpdateExpression", expr)
	if err != nil {
		return nil, err
	}
	p := &condParser{toks: toks, expr: expr, kindName: "UpdateExpression", names: names, values: values}
	prog := &updateProgram{}
	for p.peek().kind != tEnd {
		t := p.peek()
		sec := ""
		if t.kind == tIdent {
			lw := strings.ToLower(t.text)
			switch lw {
			case "set", "remove", "add", "delete":
				sec = lw
			}
		}
		if sec == "" {
			return nil, p.syntaxErrAt(t)
		}
		p.next()
		switch sec {
		case "set":
			for {
				pv, err := p.parsePath()
				if err != nil {
					return nil, err
				}
				pn, ok := pv.(*pathNode)
				if !ok {
					return nil, p.syntaxErrAt(p.peek())
				}
				if err := p.expectSym("="); err != nil {
					return nil, err
				}
				rhs, err := p.parseSetRHS()
				if err != nil {
					return nil, err
				}
				prog.sets = append(prog.sets, setAction{path: *pn, src: rhs})
				if !p.nextIfSym(",") {
					break
				}
			}
		case "remove":
			for {
				pv, err := p.parsePath()
				if err != nil {
					return nil, err
				}
				pn, ok := pv.(*pathNode)
				if !ok {
					return nil, p.syntaxErrAt(p.peek())
				}
				prog.removes = append(prog.removes, removeAction{path: *pn})
				if !p.nextIfSym(",") {
					break
				}
			}
		default: // add | delete
			for {
				pv, err := p.parsePath()
				if err != nil {
					return nil, err
				}
				pn, ok := pv.(*pathNode)
				if !ok {
					return nil, p.syntaxErrAt(p.peek())
				}
				op, err := p.parseOperand()
				if err != nil {
					return nil, err
				}
				if sec == "add" {
					prog.adds = append(prog.adds, addAction{path: *pn, operand: op})
				} else {
					prog.deletes = append(prog.deletes, deleteAction{path: *pn, operand: op})
				}
				if !p.nextIfSym(",") {
					break
				}
			}
		}
	}
	if err := prog.checkOverlaps(names); err != nil {
		return nil, err
	}
	return prog, nil
}

// parseSetRHS parses the right-hand side of a SET action: an operand, an
// arithmetic expression, if_not_exists(...) or list_append(...).
func (p *condParser) parseSetRHS() (valueNode, *APIError) {
	t := p.peek()
	if t.kind == tIdent {
		switch strings.ToLower(t.text) {
		case "if_not_exists":
			p.next()
			if !p.nextIfSym("(") {
				return nil, p.syntaxErrAt(p.peek())
			}
			inner, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			if !p.nextIfSym(",") {
				return nil, p.syntaxErrAt(p.peek())
			}
			def, err := p.parseSetRHS()
			if err != nil {
				return nil, err
			}
			if !p.nextIfSym(")") {
				return nil, p.syntaxErrAt(p.peek())
			}
			return &ifNotExistsNode{path: inner, def: def}, nil
		case "list_append":
			p.next()
			if !p.nextIfSym("(") {
				return nil, p.syntaxErrAt(p.peek())
			}
			l, err := p.parseSetRHS()
			if err != nil {
				return nil, err
			}
			if !p.nextIfSym(",") {
				return nil, p.syntaxErrAt(p.peek())
			}
			r, err := p.parseSetRHS()
			if err != nil {
				return nil, err
			}
			if !p.nextIfSym(")") {
				return nil, p.syntaxErrAt(p.peek())
			}
			return &listAppendNode{l: l, r: r}, nil
		}
	}
	return p.parseArith()
}

// checkOverlaps rejects duplicate document paths across all sections.
func (u *updateProgram) checkOverlaps(names map[string]*string) *APIError {
	seen := map[string]bool{}
	add := func(pn pathNode) *APIError {
		s := pn.pathString(names)
		if seen[s] {
			return validationErr("Two document paths overlap with each other")
		}
		seen[s] = true
		return nil
	}
	for _, a := range u.sets {
		if err := add(a.path); err != nil {
			return err
		}
	}
	for _, a := range u.removes {
		if err := add(a.path); err != nil {
			return err
		}
	}
	for _, a := range u.adds {
		if err := add(a.path); err != nil {
			return err
		}
	}
	for _, a := range u.deletes {
		if err := add(a.path); err != nil {
			return err
		}
	}
	return nil
}

// touchesKey reports the name of the key attribute written by the program,
// if any. Writes to key attributes are rejected.
func (u *updateProgram) touchesKey(hash, rng string, names map[string]*string) (string, bool) {
	check := func(pn pathNode) (string, bool) {
		nm := pn.root
		if pn.rootRef {
			s, err := lookupName(names, pn.root)
			if err != nil {
				return "", false
			}
			nm = s
		}
		if nm == hash || (rng != "" && nm == rng) {
			return nm, true
		}
		return "", false
	}
	for _, a := range u.sets {
		if n, hit := check(a.path); hit {
			return n, true
		}
	}
	for _, a := range u.removes {
		if n, hit := check(a.path); hit {
			return n, true
		}
	}
	for _, a := range u.adds {
		if n, hit := check(a.path); hit {
			return n, true
		}
	}
	for _, a := range u.deletes {
		if n, hit := check(a.path); hit {
			return n, true
		}
	}
	return "", false
}

// Path helpers -----------------------------------------------------------------

func pathRootName(names map[string]*string, p pathNode) (string, *APIError) {
	if p.rootRef {
		return lookupName(names, p.root)
	}
	return p.root, nil
}

func segName(names map[string]*string, sg pathSeg) (string, *APIError) {
	if sg.ref {
		return lookupName(names, sg.name)
	}
	return sg.name, nil
}

// setIntoMap writes val at the path given by segs, where the current
// container is the map m.
func setIntoMap(names map[string]*string, m map[string]*AttributeValue, segs []pathSeg, val *AttributeValue) *APIError {
	sg := segs[0]
	if sg.isIndex {
		return invalidPathErrUpd()
	}
	nm, err := segName(names, sg)
	if err != nil {
		return err
	}
	if len(segs) == 1 {
		m[nm] = val
		return nil
	}
	cur := m[nm]
	if cur == nil {
		return invalidPathErrUpd()
	}
	next := segs[1]
	if next.isIndex {
		if cur.TypeName() != "L" {
			return invalidPathErrUpd()
		}
		if next.index < 0 || next.index >= len(cur.L) {
			return invalidPathErrUpd()
		}
		if len(segs) == 2 {
			cur.L[next.index] = val
			return nil
		}
		elem := cur.L[next.index]
		if elem == nil || elem.TypeName() != "M" {
			return invalidPathErrUpd()
		}
		return setIntoMap(names, elem.M, segs[2:], val)
	}
	if cur.TypeName() != "M" {
		return invalidPathErrUpd()
	}
	return setIntoMap(names, cur.M, segs[1:], val)
}

// setPathValue sets the value at a document path inside an item.
func setPathValue(item map[string]*AttributeValue, names map[string]*string, p pathNode, val *AttributeValue) *APIError {
	if len(p.segs) == 0 {
		root, err := pathRootName(names, p)
		if err != nil {
			return err
		}
		item[root] = val
		return nil
	}
	if p.segs[0].isIndex {
		root, err := pathRootName(names, p)
		if err != nil {
			return err
		}
		cur, ok := item[root]
		if !ok || cur == nil {
			return invalidPathErrUpd()
		}
		if cur.TypeName() != "L" {
			return invalidPathErrUpd()
		}
		idx := p.segs[0].index
		if idx < 0 || idx >= len(cur.L) {
			return invalidPathErrUpd()
		}
		if len(p.segs) == 1 {
			cur.L[idx] = val
			return nil
		}
		elem := cur.L[idx]
		if elem == nil || elem.TypeName() != "M" {
			return invalidPathErrUpd()
		}
		return setIntoMap(names, elem.M, p.segs[1:], val)
	}
	return setIntoMap(names, item, p.segs, val)
}

func removeIntoMap(names map[string]*string, m map[string]*AttributeValue, segs []pathSeg) *APIError {
	sg := segs[0]
	if sg.isIndex {
		return invalidPathErrUpd()
	}
	nm, err := segName(names, sg)
	if err != nil {
		return err
	}
	if len(segs) == 1 {
		delete(m, nm)
		return nil
	}
	cur := m[nm]
	if cur == nil {
		return invalidPathErrUpd()
	}
	next := segs[1]
	if next.isIndex {
		if cur.TypeName() != "L" {
			return invalidPathErrUpd()
		}
		if next.index < 0 || next.index >= len(cur.L) {
			return invalidPathErrUpd()
		}
		if len(segs) == 2 {
			cur.L = append(cur.L[:next.index], cur.L[next.index+1:]...)
			return nil
		}
		elem := cur.L[next.index]
		if elem == nil || elem.TypeName() != "M" {
			return invalidPathErrUpd()
		}
		return removeIntoMap(names, elem.M, segs[2:])
	}
	if cur.TypeName() != "M" {
		return invalidPathErrUpd()
	}
	return removeIntoMap(names, cur.M, segs[1:])
}

// removePathValue removes the attribute (or list element) at a path.
func removePathValue(item map[string]*AttributeValue, names map[string]*string, p pathNode) *APIError {
	if len(p.segs) == 0 {
		root, err := pathRootName(names, p)
		if err != nil {
			return err
		}
		delete(item, root)
		return nil
	}
	if p.segs[0].isIndex {
		root, err := pathRootName(names, p)
		if err != nil {
			return err
		}
		cur, ok := item[root]
		if !ok || cur == nil || cur.TypeName() != "L" {
			return invalidPathErrUpd()
		}
		idx := p.segs[0].index
		if idx < 0 || idx >= len(cur.L) {
			return invalidPathErrUpd()
		}
		if len(p.segs) == 1 {
			cur.L = append(cur.L[:idx], cur.L[idx+1:]...)
			return nil
		}
		elem := cur.L[idx]
		if elem == nil || elem.TypeName() != "M" {
			return invalidPathErrUpd()
		}
		return removeIntoMap(names, elem.M, p.segs[1:])
	}
	return removeIntoMap(names, item, p.segs)
}

// ADD / DELETE ----------------------------------------------------------------

func setTypeName(t string) string {
	switch t {
	case "SS", "NS", "BS":
		return t
	}
	return ""
}

func setLen(v *AttributeValue) int {
	switch v.TypeName() {
	case "SS":
		return len(v.SS)
	case "NS":
		return len(v.NS)
	case "BS":
		return len(v.BS)
	}
	return 0
}

func unionSets(cur, add *AttributeValue) *AttributeValue {
	out := cloneValue(cur)
	switch cur.TypeName() {
	case "SS":
		seen := map[string]bool{}
		for _, s := range cur.SS {
			seen[s] = true
		}
		for _, s := range add.SS {
			if !seen[s] {
				out.SS = append(out.SS, s)
				seen[s] = true
			}
		}
	case "NS":
		seen := map[string]bool{}
		for _, s := range cur.NS {
			seen[canonicalNumber(s)] = true
		}
		for _, s := range add.NS {
			k := canonicalNumber(s)
			if !seen[k] {
				out.NS = append(out.NS, s)
				seen[k] = true
			}
		}
	case "BS":
		seen := map[string]bool{}
		for _, b := range cur.BS {
			seen[fmt.Sprintf("%x", b)] = true
		}
		for _, b := range add.BS {
			k := fmt.Sprintf("%x", b)
			if !seen[k] {
				out.BS = append(out.BS, b)
				seen[k] = true
			}
		}
	}
	return out
}

func subtractSets(cur, sub *AttributeValue) *AttributeValue {
	out := &AttributeValue{}
	switch cur.TypeName() {
	case "SS":
		rm := map[string]bool{}
		for _, s := range sub.SS {
			rm[s] = true
		}
		for _, s := range cur.SS {
			if !rm[s] {
				out.SS = append(out.SS, s)
			}
		}
	case "NS":
		rm := map[string]bool{}
		for _, s := range sub.NS {
			rm[canonicalNumber(s)] = true
		}
		for _, s := range cur.NS {
			if !rm[canonicalNumber(s)] {
				out.NS = append(out.NS, s)
			}
		}
	case "BS":
		rm := map[string]bool{}
		for _, b := range sub.BS {
			rm[fmt.Sprintf("%x", b)] = true
		}
		for _, b := range cur.BS {
			if !rm[fmt.Sprintf("%x", b)] {
				out.BS = append(out.BS, b)
			}
		}
	}
	return out
}

func applyAdd(e *evaluator, item map[string]*AttributeValue, a addAction) *APIError {
	ov, ok, err := e.value(a.operand)
	if err != nil {
		return err
	}
	if !ok {
		return validationErr(msgMissingAttr)
	}
	cur, exists, err := e.path(&a.path)
	if err != nil {
		return err
	}
	if !exists {
		return setPathValue(item, e.names, a.path, cloneValue(ov))
	}
	tCur, tNew := cur.TypeName(), ov.TypeName()
	switch {
	case tCur == "N" && tNew == "N":
		r1, ok1 := parseRat(cur.N)
		r2, ok2 := parseRat(ov.N)
		if !ok1 || !ok2 {
			return validationErr("An operand in the update expression has an incorrect data type")
		}
		sum := new(big.Rat).Add(r1, r2)
		return setPathValue(item, e.names, a.path, aN(FormatRat(sum)))
	case tCur == tNew && setTypeName(tCur) != "":
		return setPathValue(item, e.names, a.path, unionSets(cur, ov))
	default:
		return validationErr("An operand in the update expression has an incorrect data type")
	}
}

func applyDelete(e *evaluator, item map[string]*AttributeValue, a deleteAction) *APIError {
	ov, ok, err := e.value(a.operand)
	if err != nil {
		return err
	}
	if !ok {
		return validationErr(msgMissingAttr)
	}
	cur, exists, err := e.path(&a.path)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	tCur, tNew := cur.TypeName(), ov.TypeName()
	if tCur == tNew && setTypeName(tCur) != "" {
		res := subtractSets(cur, ov)
		if setLen(res) == 0 {
			return removePathValue(item, e.names, a.path)
		}
		return setPathValue(item, e.names, a.path, res)
	}
	return validationErr("An operand in the update expression has an incorrect data type")
}

// applyUpdate compiles and applies an UpdateExpression to the item at enc.
// Sections are applied in DynamoDB's canonical order: SET, REMOVE, ADD,
// DELETE.
func (t *Table) applyUpdate(enc string, key map[string]*AttributeValue, prog *updateProgram, names map[string]*string, values map[string]*AttributeValue) *APIError {
	base, existed := t.getItem(enc)
	item := map[string]*AttributeValue{}
	if existed {
		for k, v := range base {
			item[k] = v
		}
	} else {
		// Updating a non-existent item creates it with the key attributes.
		for k, v := range key {
			item[k] = cloneValue(v)
		}
	}
	e := &evaluator{item: item, names: names, values: values, kind: "UpdateExpression"}
	for _, a := range prog.sets {
		v, ok, err := e.value(a.src)
		if err != nil {
			return err
		}
		if !ok {
			return validationErr(msgMissingAttr)
		}
		if err := setPathValue(item, names, a.path, cloneValue(v)); err != nil {
			return err
		}
	}
	for _, a := range prog.removes {
		if err := removePathValue(item, names, a.path); err != nil {
			return err
		}
	}
	for _, a := range prog.adds {
		if err := applyAdd(e, item, a); err != nil {
			return err
		}
	}
	for _, a := range prog.deletes {
		if err := applyDelete(e, item, a); err != nil {
			return err
		}
	}
	// The key of the item must be unchanged.
	for k, v := range key {
		cur, ok := item[k]
		if !ok || !valuesEqual(cur, v) {
			return schemaErr()
		}
	}
	t.putItemRaw(enc, item)
	return nil
}
