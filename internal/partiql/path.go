package partiql

import "strconv"

// PathElem is one segment of a document path (name or [index]).
type PathElem struct {
	Name  string // attribute name (references already resolved)
	Index int
	IsIdx bool
	Orig  string // element as written ("#a", "b", "[3]")
}

// Path is an attribute path (e.g. a.b[2].#c).
type Path struct{ Elems []PathElem }

// String renders the path with resolved names (a.b[2]).
func (p Path) String() string {
	out := ""
	for i, e := range p.Elems {
		if e.IsIdx {
			out += "[" + strconv.Itoa(e.Index) + "]"
		} else {
			if i > 0 {
				out += "."
			}
			out += e.Name
		}
	}
	return out
}

func (e PathElem) key() string {
	if e.IsIdx {
		return "[" + strconv.Itoa(e.Index) + "]"
	}
	return "#n:" + e.Name
}

// samePath reports exact structural equality of two paths (used for
// duplicate detection in update actions).
func samePath(a, b Path) bool {
	if len(a.Elems) != len(b.Elems) {
		return false
	}
	for i := range a.Elems {
		x, y := a.Elems[i], b.Elems[i]
		if x.IsIdx != y.IsIdx {
			return false
		}
		if x.IsIdx {
			if x.Index != y.Index {
				return false
			}
		} else if x.Name != y.Name {
			return false
		}
	}
	return true
}

// overlapNode implements the prefix tree of DynamoDbUpdateExpressionSyntax,
// which rejects a second write to the same document path or to a prefix /
// descendant of a previously seen path.
type overlapNode struct {
	leafKinds map[string]bool // action kinds of paths that end at this node
	kids      map[string]*overlapNode
}

func newOverlapNode() *overlapNode {
	return &overlapNode{leafKinds: map[string]bool{}, kids: map[string]*overlapNode{}}
}

// insert records a path tagged with an action kind. It reports (existing
// leaf kinds at the same node, exact duplicate, overlap).
func (n *overlapNode) insert(elems []PathElem, kind string) (existing map[string]bool, dup bool, overlap bool) {
	cur := n
	for i, e := range elems {
		key := e.key()
		nx, ok := cur.kids[key]
		if !ok {
			nx = newOverlapNode()
			cur.kids[key] = nx
		}
		cur = nx
		if i+1 < len(elems) && len(cur.leafKinds) > 0 {
			return nil, false, true // ancestor of the new path is an existing leaf
		}
	}
	if len(cur.kids) > 0 {
		return cur.leafKinds, false, true // descendant already exists
	}
	if len(cur.leafKinds) > 0 {
		return cur.leafKinds, true, false
	}
	cur.leafKinds[kind] = true
	return nil, false, false
}
