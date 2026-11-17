package partiql

// This file ports floci's DynamoDbPartiQLHandler: statement execution over
// the local table store (SELECT with projection, INSERT with duplicate
// detection, UPDATE with SET/REMOVE/RETURNING, DELETE, CREATE/DROP TABLE),
// wired to the key-plan routing.

// KeySchema names a table's key attributes.
type KeySchema struct {
  Table     string
  Partition string
  Sort      string
}

// TableDef carries the table metadata needed by CREATE TABLE.
type TableDef struct {
  Table     string
  Partition string
  Sort      string
}

// TableStore is the persistence surface the PartiQL handler executes against.
type TableStore interface {
  Schema(table string) (KeySchema, error)
  ScanItems(table string) ([]map[string]*AttrValue, error)
  PartitionItems(table string, pk *AttrValue) ([]map[string]*AttrValue, error)
  GetItem(table string, key map[string]*AttrValue) (map[string]*AttrValue, bool, error)
  PutItem(table string, item map[string]*AttrValue, requireNew bool) error
  DeleteItem(table string, key map[string]*AttrValue) error
  CreateTable(def TableDef) error
  DropTable(table string) error
}

// Result reports the items returned by a statement: SELECT results, or the
// items captured by a RETURNING clause (wire: ExecuteStatement item maps).
type Result struct {
  Items []map[string]*AttrValue
}

// Count returns the number of returned items.
func (r *Result) Count() int { return len(r.Items) }

// Handler parses and executes PartiQL statements against a TableStore.
type Handler struct {
  store TableStore
}

// NewHandler wires a handler to a store.
func NewHandler(store TableStore) *Handler { return &Handler{store: store} }

// ExecuteStatement parses and executes a statement with positional parameters.
func (h *Handler) ExecuteStatement(sql string, params []*AttrValue) (*Result, error) {
  return h.Execute(sql, params, nil)
}

// Execute parses and executes a statement with positional and named parameters.
func (h *Handler) Execute(sql string, params []*AttrValue, named map[string]*AttrValue) (*Result, error) {
  stmt, err := Parse(sql)
  if err != nil {
    return nil, err
  }
  return h.ExecuteParsed(stmt, params, named)
}

// ExecuteParsed executes an already parsed statement.
func (h *Handler) ExecuteParsed(stmt Stmt, params []*AttrValue, named map[string]*AttrValue) (*Result, error) {
  ctx := &evalCtx{params: params, named: named}
  bind := binderFor(ctx)
  switch s := stmt.(type) {
  case *SelectStmt:
    return h.execSelect(s, ctx, bind)
  case *InsertStmt:
    return h.execInsert(s, ctx)
  case *UpdateStmt:
    return h.execUpdate(s, ctx, bind)
  case *DeleteStmt:
    return h.execDelete(s, ctx, bind)
  case *CreateTableStmt:
    return h.execCreate(s)
  case *DropTableStmt:
    return h.execDrop(s)
  }
  return nil, errValidation("unsupported statement type")
}

// binderFor resolves constant and parameter expressions during key planning.
func binderFor(ctx *evalCtx) paramBinder {
  return func(e Expr) (*AttrValue, error) {
    switch t := e.(type) {
    case *LitExpr:
      if t.Missing || t.Val == nil {
        return nil, errValidation("key conditions require a constant value")
      }
      return t.Val.Clone(), nil
    case *ParamExpr:
      return resolveParam(ctx, t)
    }
    return nil, errValidation("key conditions must use constants or parameters")
  }
}

// itemKey extracts the full primary key of an item.
func itemKey(schema KeySchema, item map[string]*AttrValue) (map[string]*AttrValue, error) {
  key := map[string]*AttrValue{}
  pv, ok := item[schema.Partition]
  if !ok || pv == nil {
    return nil, errValidation("item is missing the partition key attribute %q", schema.Partition)
  }
  key[schema.Partition] = pv
  if schema.Sort != "" {
    sv, ok := item[schema.Sort]
    if !ok || sv == nil {
      return nil, errValidation("item is missing the sort key attribute %q", schema.Sort)
    }
    key[schema.Sort] = sv
  }
  return key, nil
}

// collectTargets fetches the items addressed by the plan before filtering.
func (h *Handler) collectTargets(schema KeySchema, plan *QueryPlan) ([]map[string]*AttrValue, error) {
  switch plan.Mode {
  case PlanItem:
    key := map[string]*AttrValue{schema.Partition: plan.Partition}
    if plan.SortEq != nil {
      key[schema.Sort] = plan.SortEq
    }
    item, found, err := h.store.GetItem(schema.Table, key)
    if err != nil {
      return nil, err
    }
    if !found || item == nil {
      return nil, nil
    }
    return []map[string]*AttrValue{item}, nil
  case PlanPartition:
    return h.store.PartitionItems(schema.Table, plan.Partition)
  default:
    return h.store.ScanItems(schema.Table)
  }
}

// filteredTargets applies sort conditions and residual filters.
func (h *Handler) filteredTargets(ctx *evalCtx, schema KeySchema, plan *QueryPlan) ([]map[string]*AttrValue, error) {
  cands, err := h.collectTargets(schema, plan)
  if err != nil {
    return nil, err
  }
  var out []map[string]*AttrValue
  for _, item := range cands {
    ok, err := plan.Matches(ctx.withItem(item), item)
    if err != nil {
      return nil, err
    }
    if ok {
      out = append(out, item)
    }
  }
  return out, nil
}

func (h *Handler) execSelect(sel *SelectStmt, ctx *evalCtx, bind paramBinder) (*Result, error) {
  schema, err := h.store.Schema(sel.Table)
  if err != nil {
    return nil, err
  }
  c := *ctx
  c.alias = sel.Alias
  ctx = &c
  plan, err := BuildQueryPlan(sel.Where, schema, sel.Alias, bind)
  if err != nil {
    return nil, err
  }
  matched, err := h.filteredTargets(ctx, schema, plan)
  if err != nil {
    return nil, err
  }
  items := make([]map[string]*AttrValue, 0, len(matched))
  for _, it := range matched {
    if sel.Star {
      items = append(items, cloneItem(it))
      continue
    }
    doc, err := projectItem(ctx.withItem(it), sel.Project)
    if err != nil {
      return nil, err
    }
    items = append(items, doc)
  }
  return &Result{Items: items}, nil
}

// projectItem builds the projected document for a matched item.
func projectItem(c *evalCtx, proj []ProjItem) (map[string]*AttrValue, error) {
  doc := map[string]*AttrValue{}
  for _, p := range proj {
    v, found, err := resolvePath(c, p.Path)
    if err != nil {
      return nil, err
    }
    if !found || v == nil {
      continue
    }
    if p.Alias != "" {
      doc[p.Alias] = v.Clone()
      continue
    }
    if err := insertProjection(doc, *p.Path, c.alias, v); err != nil {
      return nil, err
    }
  }
  return doc, nil
}

// insertProjection writes v at the (possibly nested) location described by
// path, merging projections that share a common prefix.
func insertProjection(doc map[string]*AttrValue, path PathExpr, alias string, v *AttrValue) error {
  root, steps, err := resolveBase(&path, alias)
  if err != nil {
    return err
  }
  if len(steps) == 0 {
    doc[root] = v.Clone()
    return nil
  }
  existing, ok := doc[root]
  if !ok || existing == nil || existing.Type != "M" {
    existing = MVal(map[string]*AttrValue{})
    doc[root] = existing
  }
  cur := existing
  for i := 0; i < len(steps)-1; i++ {
    st := steps[i]
    if st.IsIdx {
      return errValidation("list indexes are not supported in projections")
    }
    child, ok := cur.M[st.Key]
    if !ok || child == nil || child.Type != "M" {
      child = MVal(map[string]*AttrValue{})
      cur.M[st.Key] = child
    }
    cur = child
  }
  last := steps[len(steps)-1]
  if last.IsIdx {
    return errValidation("list indexes are not supported in projections")
  }
  cur.M[last.Key] = v.Clone()
  return nil
}

func (h *Handler) execInsert(ins *InsertStmt, ctx *evalCtx) (*Result, error) {
  schema, err := h.store.Schema(ins.Table)
  if err != nil {
    return nil, err
  }
  if ins.Returning != nil && ins.Returning.Version == "OLD" {
    return nil, errValidation("RETURNING OLD is not valid for INSERT")
  }
  inserted := make([]map[string]*AttrValue, 0, len(ins.Docs))
  for _, doc := range ins.Docs {
    item := make(map[string]*AttrValue, len(doc.Fields))
    for _, f := range doc.Fields {
      if _, dup := item[f.Key]; dup {
        return nil, errValidation("duplicate attribute %q in INSERT document", f.Key)
      }
      v, err := evalValue(ctx, f.Val)
      if err != nil {
        return nil, err
      }
      if v == nil {
        return nil, errValidation("missing value for attribute %q", f.Key)
      }
      item[f.Key] = v
    }
    if _, err := itemKey(schema, item); err != nil {
      return nil, err
    }
    if err := h.store.PutItem(ins.Table, item, true); err != nil {
      return nil, err
    }
    inserted = append(inserted, cloneItem(item))
  }
  res := &Result{}
  if ins.Returning != nil {
    res.Items = inserted
  }
  return res, nil
}

// validateUpdateTargets rejects writes to key attributes.
func validateUpdateTargets(schema KeySchema, u *UpdateStmt) error {
  isKey := func(name string) bool {
    return name == schema.Partition || (schema.Sort != "" && name == schema.Sort)
  }
  for _, s := range u.Sets {
    if isKey(s.Path.Root) {
      return errValidation("key attribute %s cannot be updated", s.Path.Root)
    }
  }
  for _, r := range u.Removes {
    if isKey(r.Root) {
      return errValidation("key attribute %s cannot be removed", r.Root)
    }
  }
  return nil
}

func (h *Handler) execUpdate(u *UpdateStmt, ctx *evalCtx, bind paramBinder) (*Result, error) {
  schema, err := h.store.Schema(u.Table)
  if err != nil {
    return nil, err
  }
  if err := validateUpdateTargets(schema, u); err != nil {
    return nil, err
  }
  plan, err := BuildQueryPlan(u.Where, schema, "", bind)
  if err != nil {
    return nil, err
  }
  matched, err := h.filteredTargets(ctx, schema, plan)
  if err != nil {
    return nil, err
  }
  res := &Result{}
  for _, item := range matched {
    updated, changed, err := applyUpdateActions(item, u.Sets, u.Removes, ctx)
    if err != nil {
      return nil, err
    }
    if changed {
      if err := h.store.PutItem(u.Table, updated, false); err != nil {
        return nil, err
      }
    }
    if u.Returning != nil {
      if u.Returning.Scope == "MODIFIED" && !changed {
        continue
      }
      if u.Returning.Version == "OLD" {
        res.Items = append(res.Items, cloneItem(item))
      } else {
        res.Items = append(res.Items, cloneItem(updated))
      }
    }
  }
  return res, nil
}

func (h *Handler) execDelete(d *DeleteStmt, ctx *evalCtx, bind paramBinder) (*Result, error) {
  schema, err := h.store.Schema(d.Table)
  if err != nil {
    return nil, err
  }
  if d.Returning != nil && d.Returning.Version == "NEW" {
    return nil, errValidation("RETURNING NEW is not valid for DELETE")
  }
  plan, err := BuildQueryPlan(d.Where, schema, "", bind)
  if err != nil {
    return nil, err
  }
  matched, err := h.filteredTargets(ctx, schema, plan)
  if err != nil {
    return nil, err
  }
  res := &Result{}
  for _, item := range matched {
    key, err := itemKey(schema, item)
    if err != nil {
      return nil, err
    }
    if err := h.store.DeleteItem(d.Table, key); err != nil {
      return nil, err
    }
    if d.Returning != nil {
      res.Items = append(res.Items, cloneItem(item))
    }
  }
  return res, nil
}

func (h *Handler) execCreate(ct *CreateTableStmt) (*Result, error) {
  def := TableDef{Table: ct.Table}
  for _, k := range ct.Keys {
    switch k.Role {
    case "HASH":
      if def.Partition != "" {
        return nil, errValidation("multiple HASH key attributes")
      }
      def.Partition = k.Name
    case "RANGE":
      if def.Sort != "" {
        return nil, errValidation("multiple RANGE key attributes")
      }
      def.Sort = k.Name
    }
  }
  if def.Partition == "" {
    return nil, errValidation("CREATE TABLE requires a HASH key attribute")
  }
  if err := h.store.CreateTable(def); err != nil {
    return nil, err
  }
  return &Result{}, nil
}

func (h *Handler) execDrop(d *DropTableStmt) (*Result, error) {
  if err := h.store.DropTable(d.Table); err != nil {
    return nil, err
  }
  return &Result{}, nil
}
