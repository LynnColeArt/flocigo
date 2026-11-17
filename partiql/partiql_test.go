package partiql

import (
  "strings"
  "testing"
)

func newTestHandler(t *testing.T) (*MemStore, *Handler) {
  t.Helper()
  store := NewMemStore()
  h := NewHandler(store)
  if _, err := h.ExecuteStatement("CREATE TABLE Music (Artist STRING, SongTitle STRING, Awards NUMBER) PRIMARY KEY (Artist HASH, SongTitle RANGE)", nil); err != nil {
    t.Fatalf("create table: %v", err)
  }
  docs := []string{
    "INSERT INTO Music VALUE {'Artist': 'AC/DC', 'SongTitle': 'Zeta', 'Awards': 2}",
    "INSERT INTO Music VALUE {'Artist': 'AC/DC', 'SongTitle': 'Alpha', 'Awards': 3}",
    "INSERT INTO Music VALUE {'Artist': 'Beatles', 'SongTitle': 'Let It Be', 'Awards': 1}",
  }
  for _, d := range docs {
    if _, err := h.ExecuteStatement(d, nil); err != nil {
      t.Fatalf("insert: %v", err)
    }
  }
  return store, h
}

func TestKeyPlanModes(t *testing.T) {
  schema := KeySchema{Table: "Music", Partition: "Artist", Sort: "SongTitle"}
  bind := func(e Expr) (*AttrValue, error) {
    switch x := e.(type) {
    case *LitExpr:
      if x.Missing || x.Val == nil {
        return nil, errValidation("bad key value")
      }
      return x.Val.Clone(), nil
    case *ParamExpr:
      return SVal("param"), nil
    }
    return nil, errValidation("bad key expression")
  }
  cases := []struct {
    where     string
    mode      string
    sortConds int
  }{
    {"Artist = 'a'", PlanPartition, 0},
    {"Artist = 'a' AND SongTitle = 's'", PlanItem, 1},
    {"Artist = 'a' AND SongTitle >= 's'", PlanPartition, 1},
    {"Artist = 'a' AND SongTitle BETWEEN 'a' AND 'z'", PlanPartition, 1},
    {"Artist = 'a' AND begins_with(SongTitle, 's')", PlanPartition, 1},
    {"Artist = 'a' OR SongTitle = 's'", PlanScan, 0},
    {"SongTitle = 's'", PlanScan, 0},
    {"Awards > 1", PlanScan, 0},
  }
  for _, tc := range cases {
    stmt, err := Parse("SELECT * FROM Music WHERE " + tc.where)
    if err != nil {
      t.Fatalf("parse %q: %v", tc.where, err)
    }
    plan, err := BuildQueryPlan(stmt.(*SelectStmt).Where, schema, "", bind)
    if err != nil {
      t.Fatalf("plan %q: %v", tc.where, err)
    }
    if plan.Mode != tc.mode {
      t.Errorf("%q: mode = %s, want %s", tc.where, plan.Mode, tc.mode)
    }
    if len(plan.SortConds) != tc.sortConds {
      t.Errorf("%q: sort conds = %d, want %d", tc.where, len(plan.SortConds), tc.sortConds)
    }
    if tc.mode != PlanScan && plan.Partition == nil {
      t.Errorf("%q: missing routed partition value", tc.where)
    }
  }
}

func TestSelectRouting(t *testing.T) {
  _, h := newTestHandler(t)
  // Partition route: both AC/DC songs, sorted by sort key.
  res, err := h.ExecuteStatement("SELECT * FROM Music WHERE Artist = 'AC/DC'", nil)
  if err != nil {
    t.Fatalf("select: %v", err)
  }
  if res.Count() != 2 {
    t.Fatalf("got %d items", res.Count())
  }
  if res.Items[0]["SongTitle"].S != "Alpha" || res.Items[1]["SongTitle"].S != "Zeta" {
    t.Fatalf("partition not sorted by sort key")
  }
  // Exact item route via parameterized primary key.
  res, err = h.ExecuteStatement("SELECT * FROM Music WHERE Artist = ? AND SongTitle = ?", []*AttrValue{SVal("AC/DC"), SVal("Alpha")})
  if err != nil {
    t.Fatalf("select item: %v", err)
  }
  if res.Count() != 1 || res.Items[0]["Awards"].N != "3" {
    t.Fatalf("unexpected item route result")
  }
}

func TestInsertDuplicateRejected(t *testing.T) {
  _, h := newTestHandler(t)
  _, err := h.ExecuteStatement("INSERT INTO Music VALUE {'Artist': 'AC/DC', 'SongTitle': 'Alpha'}", nil)
  if err == nil || !strings.Contains(err.Error(), "duplicate key exists") {
    t.Fatalf("expected duplicate-key error, got %v", err)
  }
}

func TestSelectProjection(t *testing.T) {
  store := NewMemStore()
  h := NewHandler(store)
  if _, err := h.ExecuteStatement("CREATE TABLE Books (BookName STRING) PRIMARY KEY (BookName HASH)", nil); err != nil {
    t.Fatalf("create: %v", err)
  }
  if _, err := h.ExecuteStatement("INSERT INTO Books VALUE {'BookName': 'b1', 'Meta': {'Year': 1999, 'Rating': 4}, 'Note': 'x'}", nil); err != nil {
    t.Fatalf("insert: %v", err)
  }
  res, err := h.ExecuteStatement("SELECT Meta.Year FROM Books WHERE BookName = 'b1'", nil)
  if err != nil {
    t.Fatalf("select: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("count %d", res.Count())
  }
  doc := res.Items[0]
  if len(doc) != 1 || doc["Meta"] == nil || doc["Meta"].M["Year"].N != "1999" || doc["Meta"].M["Rating"] != nil {
    t.Fatalf("bad nested projection: %v", doc)
  }
  res, err = h.ExecuteStatement("SELECT * FROM Books WHERE BookName = 'b1'", nil)
  if err != nil || res.Count() != 1 || len(res.Items[0]) != 3 {
    t.Fatalf("star projection lost attributes")
  }
  res, err = h.ExecuteStatement("SELECT Ghost FROM Books WHERE BookName = 'b1'", nil)
  if err != nil || res.Count() != 1 || len(res.Items[0]) != 0 {
    t.Fatalf("missing attribute must be omitted")
  }
}

func TestConditionFunctionsAndScan(t *testing.T) {
  _, h := newTestHandler(t)
  check := func(sql string, want int) {
    t.Helper()
    res, err := h.ExecuteStatement(sql, nil)
    if err != nil {
      t.Fatalf("%s: %v", sql, err)
    }
    if res.Count() != want {
      t.Fatalf("%s: got %d items, want %d", sql, res.Count(), want)
    }
  }
  check("SELECT * FROM Music WHERE contains(SongTitle, 'eta')", 1)
  check("SELECT * FROM Music WHERE size(SongTitle) > 7", 1)
  check("SELECT * FROM Music WHERE attribute_not_exists(Awards)", 0)
  check("SELECT * FROM Music WHERE Awards BETWEEN 2 AND 3", 2)
  check("SELECT * FROM Music WHERE SongTitle IN ('Alpha', 'Let It Be')", 2)
  check("SELECT * FROM Music WHERE NOT (Artist = 'AC/DC')", 1)
  check("SELECT * FROM Music WHERE Artist = 'AC/DC' AND Awards >= 2", 2)
}

func TestUpdateRoutingAndExpressions(t *testing.T) {
  _, h := newTestHandler(t)
  if _, err := h.ExecuteStatement("UPDATE Music SET Awards = Awards + ? WHERE Artist = ? AND SongTitle = ?", []*AttrValue{NVal("10"), SVal("AC/DC"), SVal("Alpha")}); err != nil {
    t.Fatalf("update: %v", err)
  }
  res, err := h.ExecuteStatement("SELECT Awards FROM Music WHERE Artist = ? AND SongTitle = ?", []*AttrValue{SVal("AC/DC"), SVal("Alpha")})
  if err != nil {
    t.Fatalf("select: %v", err)
  }
  if res.Count() != 1 || res.Items[0]["Awards"].N != "13" {
    t.Fatalf("awards arithmetic failed")
  }
  if _, err := h.ExecuteStatement("UPDATE Music SET Artist = 'X' WHERE Artist = 'AC/DC' AND SongTitle = 'Alpha'", nil); err == nil {
    t.Fatalf("updating a key attribute must fail")
  }
  if _, err := h.ExecuteStatement("UPDATE Music REMOVE Awards WHERE Artist = ? AND SongTitle = ?", []*AttrValue{SVal("AC/DC"), SVal("Zeta")}); err != nil {
    t.Fatalf("remove: %v", err)
  }
  res, err = h.ExecuteStatement("SELECT * FROM Music WHERE Artist = ? AND SongTitle = ?", []*AttrValue{SVal("AC/DC"), SVal("Zeta")})
  if err != nil {
    t.Fatalf("select after remove: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("expected one item, got %d", res.Count())
  }
  if _, ok := res.Items[0]["Awards"]; ok {
    t.Fatalf("Awards should have been removed")
  }
}

func TestUpdateReturning(t *testing.T) {
  _, h := newTestHandler(t)
  res, err := h.ExecuteStatement("UPDATE Music SET Awards = 9 WHERE Artist = 'Beatles' RETURNING ALL OLD ITEMS", nil)
  if err != nil {
    t.Fatalf("update: %v", err)
  }
  if res.Count() != 1 || res.Items[0]["Awards"].N != "1" {
    t.Fatalf("old item result wrong")
  }
  res, err = h.ExecuteStatement("SELECT Awards FROM Music WHERE Artist = 'Beatles'", nil)
  if err != nil || res.Count() != 1 || res.Items[0]["Awards"].N != "9" {
    t.Fatalf("update not persisted")
  }
  res, err = h.ExecuteStatement("UPDATE Music SET Awards = Awards WHERE Artist = 'Beatles' RETURNING MODIFIED NEW ITEMS", nil)
  if err != nil {
    t.Fatalf("update: %v", err)
  }
  if res.Count() != 0 {
    t.Fatalf("unchanged update must not be reported as modified")
  }
  res, err = h.ExecuteStatement("UPDATE Music SET Awards = Awards WHERE Artist = 'Beatles' RETURNING ALL NEW ITEMS", nil)
  if err != nil {
    t.Fatalf("update: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("all-new-items count %d", res.Count())
  }
}

func TestDeleteWithReturning(t *testing.T) {
  _, h := newTestHandler(t)
  res, err := h.ExecuteStatement("DELETE FROM Music WHERE Artist = 'AC/DC' RETURNING ALL OLD ITEMS", nil)
  if err != nil {
    t.Fatalf("delete: %v", err)
  }
  if res.Count() != 2 {
    t.Fatalf("deleted %d items", res.Count())
  }
  res, err = h.ExecuteStatement("SELECT * FROM Music", nil)
  if err != nil || res.Count() != 1 || res.Items[0]["Artist"].S != "Beatles" {
    t.Fatalf("unexpected remaining items")
  }
}

func TestUpdateNestedAndListAppend(t *testing.T) {
  store := NewMemStore()
  h := NewHandler(store)
  if _, err := h.ExecuteStatement("CREATE TABLE Books (BookName STRING) PRIMARY KEY (BookName HASH)", nil); err != nil {
    t.Fatalf("create: %v", err)
  }
  if _, err := h.ExecuteStatement("INSERT INTO Books VALUE {'BookName': 'b1', 'Tags': ['a']}", nil); err != nil {
    t.Fatalf("insert: %v", err)
  }
  if _, err := h.ExecuteStatement("UPDATE Books SET Meta.Year = 2020, Tags = list_append(Tags, ?) WHERE BookName = 'b1'", []*AttrValue{LVal([]*AttrValue{SVal("b")})}); err != nil {
    t.Fatalf("update: %v", err)
  }
  res, err := h.ExecuteStatement("SELECT * FROM Books WHERE BookName = 'b1'", nil)
  if err != nil || res.Count() != 1 {
    t.Fatalf("select: %v", err)
  }
  item := res.Items[0]
  if item["Meta"].M["Year"].N != "2020" {
    t.Fatalf("nested set failed")
  }
  if len(item["Tags"].L) != 2 || item["Tags"].L[1].S != "b" {
    t.Fatalf("list_append failed")
  }
  res, err = h.ExecuteStatement("SELECT BookName FROM Books WHERE contains(Tags, 'b') AND size(Tags) = 2", nil)
  if err != nil {
    t.Fatalf("select: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("contains/size filter failed")
  }
}

func TestParameterForms(t *testing.T) {
  _, h := newTestHandler(t)
  res, err := h.Execute("SELECT * FROM Music WHERE Artist = ?1 AND SongTitle = ?0", []*AttrValue{SVal("Alpha"), SVal("AC/DC")}, nil)
  if err != nil {
    t.Fatalf("indexed params: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("indexed params: got %d items", res.Count())
  }
  res, err = h.Execute("SELECT * FROM Music WHERE Artist = :a AND SongTitle = :s", nil, map[string]*AttrValue{"a": SVal("AC/DC"), "s": SVal("Alpha")})
  if err != nil {
    t.Fatalf("named params: %v", err)
  }
  if res.Count() != 1 {
    t.Fatalf("named params: got %d items", res.Count())
  }
  if _, err := h.ExecuteStatement("SELECT * FROM Music WHERE Artist = ?", nil); err == nil {
    t.Fatalf("missing parameter must fail")
  }
}

func TestCreateAndDropErrors(t *testing.T) {
  store := NewMemStore()
  h := NewHandler(store)
  if _, err := h.ExecuteStatement("CREATE TABLE T1 (id STRING) PRIMARY KEY (id HASH)", nil); err != nil {
    t.Fatalf("create: %v", err)
  }
  _, err := h.ExecuteStatement("CREATE TABLE T1 (id STRING) PRIMARY KEY (id HASH)", nil)
  if err == nil || !strings.Contains(err.Error(), "already exists") {
    t.Fatalf("expected in-use error, got %v", err)
  }
  if _, err := h.ExecuteStatement("INSERT INTO T1 VALUE {'other': 1}", nil); err == nil {
    t.Fatalf("missing primary key must fail")
  }
  if _, err := h.ExecuteStatement("DROP TABLE T1", nil); err != nil {
    t.Fatalf("drop: %v", err)
  }
  if _, err := h.ExecuteStatement("SELECT * FROM T1", nil); err == nil || !strings.Contains(err.Error(), "not found") {
    t.Fatalf("expected not-found error, got %v", err)
  }
}

func TestParseErrors(t *testing.T) {
  if _, err := Parse("SELECT FROM Music"); err == nil {
    t.Fatal("bad projection must fail")
  }
  if _, err := Parse("INSERT INTO Music VALUE"); err == nil {
    t.Fatal("malformed INSERT must fail")
  }
  if _, err := Parse("SELECTA * FROM Music"); err == nil {
    t.Fatal("unknown statement must fail")
  }
  if _, err := Parse(""); err == nil {
    t.Fatal("empty statement must fail")
  }
}
