package partiql

import (
  "bytes"
  "strings"
)

// This file ports floci's DynamoDbPartiQLKeyPlan (with the condition-key
// extraction of DynamoDbConditionKeys): it classifies a WHERE clause into
// routed key conditions (exact partition, optional sort refinement) and
// residual filters evaluated per item.

// Plan modes produced by BuildQueryPlan.
const (
  PlanItem      = "ITEM"      // single item addressed by the full primary key
  PlanPartition = "PARTITION" // one partition addressed by the partition key
  PlanScan      = "SCAN"      // full table scan filtered per item
)

// paramBinder resolves constant/parameter expressions during planning.
type paramBinder func(Expr) (*AttrValue, error)

// keyCondition is a routed condition on a key attribute.
type keyCondition struct {
  Attr string
  Op   string // "=", "<", "<=", ">", ">=", "BETWEEN", "BEGINS_WITH"
  Args []*AttrValue
}

// QueryPlan describes how a statement locates its target items.
type QueryPlan struct {
  Mode      string
  Partition *AttrValue     // routed partition value (nil for scans)
  SortEq    *AttrValue     // exact sort value when Mode == PlanItem
  SortConds []keyCondition // routed sort key conditions (applied as filters)
  Filter    []Expr         // residual conditions evaluated per item
}

// BuildQueryPlan classifies the WHERE clause into routed key conditions and
// residual filters. Only top-level AND conjuncts with equality on the
// partition key route the statement; sort key equality/ranges/BETWEEN/
// begins_with refine within the partition.
func BuildQueryPlan(where Expr, schema KeySchema, alias string, bind paramBinder) (*QueryPlan, error) {
  if where == nil {
    return &QueryPlan{Mode: PlanScan}, nil
  }
  conjuncts := flattenAnd(where)
  var partition *AttrValue
  var skEq *AttrValue
  var skConds []keyCondition
  var residual []Expr
  for _, e := range conjuncts {
    used := false
    if cond, ok := binaryKeyCond(e, schema, alias, bind); ok {
      switch {
      case cond.Op == "=" && cond.Attr == schema.Partition && partition == nil && isKeyScalar(cond.Args[0]):
        partition = cond.Args[0]
        used = true
      case schema.Sort != "" && cond.Attr == schema.Sort && isKeyScalar(cond.Args[0]):
        if cond.Op == "=" && skEq == nil {
          skEq = cond.Args[0]
          skConds = append(skConds, cond)
          used = true
        } else if cond.Op != "=" {
          skConds = append(skConds, cond)
          used = true
        }
      }
    }
    if !used {
      if cond, ok := matchBetween(e, schema, alias, bind); ok && schema.Sort != "" && cond.Attr == schema.Sort {
        skConds = append(skConds, cond)
        used = true
      }
    }
    if !used {
      if cond, ok := matchBeginsWith(e, schema, alias, bind); ok && schema.Sort != "" && cond.Attr == schema.Sort {
        skConds = append(skConds, cond)
        used = true
      }
    }
    if !used {
      residual = append(residual, e)
    }
  }
  if partition == nil {
    // Nothing is routed when the partition key is not pinned; every
    // condition (including sort-key conditions) becomes a scan filter.
    return &QueryPlan{Mode: PlanScan, Filter: conjuncts}, nil
  }
  mode := PlanPartition
  if skEq != nil {
    mode = PlanItem
  }
  return &QueryPlan{Mode: mode, Partition: partition, SortEq: skEq, SortConds: skConds, Filter: residual}, nil
}

// Matches applies the plan's sort conditions and residual filters to item.
func (q *QueryPlan) Matches(c *evalCtx, item map[string]*AttrValue) (bool, error) {
  for _, kc := range q.SortConds {
    v, ok := item[kc.Attr]
    if !ok || !keyCondPasses(kc, v) {
      return false, nil
    }
  }
  for _, e := range q.Filter {
    pass, err := evalBool(c, e)
    if err != nil {
      return false, err
    }
    if !pass {
      return false, nil
    }
  }
  return true, nil
}

func flattenAnd(e Expr) []Expr {
  if b, ok := e.(*BinExpr); ok && b.Op == "AND" {
    return append(flattenAnd(b.L), flattenAnd(b.R)...)
  }
  return []Expr{e}
}

// binaryKeyCond matches "<path> <cmpop> <constant>" in either orientation.
func binaryKeyCond(e Expr, schema KeySchema, alias string, bind paramBinder) (keyCondition, bool) {
  b, ok := e.(*BinExpr)
  if !ok {
    return keyCondition{}, false
  }
  switch b.Op {
  case "=", "<", "<=", ">", ">=":
  default:
    return keyCondition{}, false
  }
  lAttr, lok := pathAtom(b.L, alias)
  rAttr, rok := pathAtom(b.R, alias)
  var attr string
  var val Expr
  switch {
  case lok && !rok:
    attr, val = lAttr, b.R
  case rok && !lok:
    attr, val = rAttr, b.L
  default:
    return keyCondition{}, false
  }
  if attr != schema.Partition && (schema.Sort == "" || attr != schema.Sort) {
    return keyCondition{}, false
  }
  v, err := bind(val)
  if err != nil {
    return keyCondition{}, false
  }
  if !isKeyScalar(v) {
    return keyCondition{}, false
  }
  return keyCondition{Attr: attr, Op: b.Op, Args: []*AttrValue{v}}, true
}

// pathAtom extracts a top level attribute reference from an expression.
func pathAtom(e Expr, alias string) (string, bool) {
  p, ok := e.(*PathExpr)
  if !ok {
    return "", false
  }
  if alias != "" && p.Root == alias && len(p.Steps) == 1 && !p.Steps[0].IsIdx {
    return p.Steps[0].Key, true
  }
  if len(p.Steps) != 0 {
    return "", false
  }
  return p.Root, true
}

func matchBetween(e Expr, schema KeySchema, alias string, bind paramBinder) (keyCondition, bool) {
  b, ok := e.(*BetweenExpr)
  if !ok || b.Neg {
    return keyCondition{}, false
  }
  attr, ok := pathAtom(b.X, alias)
  if !ok || schema.Sort == "" || attr != schema.Sort {
    return keyCondition{}, false
  }
  lo, err := bind(b.Lo)
  if err != nil || !isKeyScalar(lo) {
    return keyCondition{}, false
  }
  hi, err := bind(b.Hi)
  if err != nil || !isKeyScalar(hi) {
    return keyCondition{}, false
  }
  return keyCondition{Attr: attr, Op: "BETWEEN", Args: []*AttrValue{lo, hi}}, true
}

func matchBeginsWith(e Expr, schema KeySchema, alias string, bind paramBinder) (keyCondition, bool) {
  f, ok := e.(*FuncExpr)
  if !ok || f.Name != "begins_with" || len(f.Args) != 2 {
    return keyCondition{}, false
  }
  attr, ok := pathAtom(f.Args[0], alias)
  if !ok || schema.Sort == "" || attr != schema.Sort {
    return keyCondition{}, false
  }
  v, err := bind(f.Args[1])
  if err != nil || !isKeyScalar(v) {
    return keyCondition{}, false
  }
  return keyCondition{Attr: attr, Op: "BEGINS_WITH", Args: []*AttrValue{v}}, true
}

func keyCondPasses(kc keyCondition, v *AttrValue) bool {
  switch kc.Op {
  case "=":
    return EqualAttr(v, kc.Args[0])
  case "<", "<=", ">", ">=":
    cmp, err := CompareAttr(v, kc.Args[0])
    if err != nil {
      return false
    }
    switch kc.Op {
    case "<":
      return cmp < 0
    case "<=":
      return cmp <= 0
    case ">":
      return cmp > 0
    case ">=":
      return cmp >= 0
    }
  case "BETWEEN":
    lo, err := CompareAttr(v, kc.Args[0])
    if err != nil {
      return false
    }
    hi, err := CompareAttr(v, kc.Args[1])
    if err != nil {
      return false
    }
    return lo >= 0 && hi <= 0
  case "BEGINS_WITH":
    if v.Type == "S" && kc.Args[0].Type == "S" {
      return strings.HasPrefix(v.S, kc.Args[0].S)
    }
    if v.Type == "B" && kc.Args[0].Type == "B" {
      return bytes.HasPrefix(v.B, kc.Args[0].B)
    }
  }
  return false
}
