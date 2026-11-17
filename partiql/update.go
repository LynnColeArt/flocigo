package partiql

// This file ports the update-expression semantics used by the PartiQL UPDATE
// handler (floci DynamoDbUpdateExpressionSyntax): SET with nested paths,
// list index assignment/appending, list_append/if_not_exists and numeric
// arithmetic on the right-hand side, plus REMOVE with list splicing.

// applyUpdateActions applies SET/REMOVE actions. Right-hand side expressions
// are evaluated against the original item; mutations accumulate on a clone.
// It reports whether the resulting item differs from the original.
func applyUpdateActions(orig map[string]*AttrValue, sets []SetAction, removes []*PathExpr, c *evalCtx) (map[string]*AttrValue, bool, error) {
  updated := cloneItem(orig)
  evalC := c.withItem(orig)
  for _, s := range sets {
    if err := applySet(updated, s.Path, s.Value, evalC); err != nil {
      return nil, false, err
    }
  }
  for _, r := range removes {
    if err := removePath(evalC, updated, r); err != nil {
      return nil, false, err
    }
  }
  changed := !equalItems(orig, updated)
  return updated, changed, nil
}

func cloneItem(m map[string]*AttrValue) map[string]*AttrValue {
  out := make(map[string]*AttrValue, len(m))
  for k, v := range m {
    out[k] = v.Clone()
  }
  return out
}

func equalItems(a, b map[string]*AttrValue) bool {
  if len(a) != len(b) {
    return false
  }
  for k, av := range a {
    bv, ok := b[k]
    if !ok || !EqualAttr(av, bv) {
      return false
    }
  }
  return true
}

// applySet evaluates the assignment value and writes it at the target path.
func applySet(item map[string]*AttrValue, path *PathExpr, valE Expr, c *evalCtx) error {
  val, err := evalValue(c, valE)
  if err != nil {
    return err
  }
  root, steps, err := resolveBase(path, c.alias)
  if err != nil {
    return err
  }
  if len(steps) == 0 {
    item[root] = val
    return nil
  }
  return setPathVal(item, root, steps, val, c)
}

// setPathVal writes val at root+steps, creating missing intermediate
// containers (maps for key steps, lists for index steps).
func setPathVal(item map[string]*AttrValue, root string, steps []PathStep, val *AttrValue, c *evalCtx) error {
  if len(steps) == 0 {
    item[root] = val
    return nil
  }
  child, ok := item[root]
  if !ok || child == nil {
    if steps[0].IsIdx {
      child = LVal(nil)
    } else {
      child = MVal(map[string]*AttrValue{})
    }
    item[root] = child
  }
  return setInto(child, steps, val, c, root)
}

func isContainer(t string) bool { return t == "M" || t == "L" }

// setInto writes val at the end of steps inside container. Scalars reached at
// intermediate positions are replaced with containers, mirroring the
// document-creating behavior of DynamoDB SET on nested paths.
func setInto(container *AttrValue, steps []PathStep, val *AttrValue, c *evalCtx, display string) error {
  st := steps[0]
  if st.IsIdx {
    if container.Type != "L" {
      return errValidation("path %s: expected a list, found %s", display, container.Type)
    }
    idx, err := stepIndex(c, st)
    if err != nil {
      return err
    }
    if len(steps) == 1 {
      if idx < 0 || idx > len(container.L) {
        return errValidation("list index %d out of range", idx)
      }
      if idx == len(container.L) {
        container.L = append(container.L, val)
      } else {
        container.L[idx] = val
      }
      return nil
    }
    if idx < 0 || idx >= len(container.L) {
      return errValidation("list index %d out of range", idx)
    }
    child := container.L[idx]
    if child == nil || !isContainer(child.Type) {
      if steps[1].IsIdx {
        child = LVal(nil)
      } else {
        child = MVal(map[string]*AttrValue{})
      }
      container.L[idx] = child
    }
    return setInto(child, steps[1:], val, c, display)
  }
  if container.Type != "M" {
    return errValidation("path %s: expected a map, found %s", display, container.Type)
  }
  if len(steps) == 1 {
    container.M[st.Key] = val
    return nil
  }
  child, ok := container.M[st.Key]
  if !ok || child == nil || !isContainer(child.Type) {
    if steps[1].IsIdx {
      child = LVal(nil)
    } else {
      child = MVal(map[string]*AttrValue{})
    }
    container.M[st.Key] = child
  }
  return setInto(child, steps[1:], val, c, display)
}

// removePath deletes the value at the given path (REMOVE semantics): map
// entries are deleted, list elements are spliced out; missing targets are a
// no-op.
func removePath(c *evalCtx, item map[string]*AttrValue, path *PathExpr) error {
  root, steps, err := resolveBase(path, c.alias)
  if err != nil {
    return err
  }
  if len(steps) == 0 {
    delete(item, root)
    return nil
  }
  parent, err := findContainer(c, item, root, steps[:len(steps)-1])
  if err != nil || parent == nil {
    return err
  }
  last := steps[len(steps)-1]
  if last.IsIdx {
    if parent.Type != "L" {
      return nil
    }
    idx, err := stepIndex(c, last)
    if err != nil {
      return err
    }
    if idx < 0 || idx >= len(parent.L) {
      return nil
    }
    parent.L = append(parent.L[:idx], parent.L[idx+1:]...)
    return nil
  }
  if parent.Type != "M" {
    return nil
  }
  delete(parent.M, last.Key)
  return nil
}

// findContainer resolves root+inter to a container value, or nil when the
// path does not exist.
func findContainer(c *evalCtx, item map[string]*AttrValue, root string, inter []PathStep) (*AttrValue, error) {
  v, ok := item[root]
  if !ok || v == nil {
    return nil, nil
  }
  cur := v
  for _, st := range inter {
    if cur == nil {
      return nil, nil
    }
    if st.IsIdx {
      if cur.Type != "L" {
        return nil, nil
      }
      idx, err := stepIndex(c, st)
      if err != nil {
        return nil, err
      }
      if idx < 0 || idx >= len(cur.L) {
        return nil, nil
      }
      cur = cur.L[idx]
      continue
    }
    if cur.Type != "M" {
      return nil, nil
    }
    nv, ok2 := cur.M[st.Key]
    if !ok2 {
      return nil, nil
    }
    cur = nv
  }
  return cur, nil
}
