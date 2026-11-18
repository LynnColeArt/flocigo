package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T) *DB {
	t.Helper()
	db := NewDB()

	music := db.CreateTable(`Music`, `Artist`, `SongTitle`, map[string]string{`Artist`: `S`, `SongTitle`: `S`})
	mustPut(t, music, Item{
		`Artist`:    sAttr(`The Acme Band`),
		`SongTitle`: sAttr(`Promo`),
	})
	mustPut(t, music, Item{
		`Artist`:     sAttr(`The Acme Band`),
		`SongTitle`:  sAttr(`Look Out, World`),
		`AlbumTitle`: sAttr(`Songs About Bees`),
		`Year`:       nAttr(`1984`),
		`Price`:      nAttr(`1.99`),
		`Genre`: mAttr(map[string]*AttributeValue{
			`Primary`: sAttr(`Rock`),
			`Styles`:  ssAttr(`Pop`, `Punk`),
		}),
		`Tags`: lAttr([]*AttributeValue{sAttr(`first`), sAttr(`second`)}),
		`Blob`: bAttr([]byte{1, 2}),
		`Bare`: nullAttr(),
		`Flag`: boolAttr(true),
	})

	singles := db.CreateTable(`Singles`, `pk`, ``, map[string]string{`pk`: `S`})
	mustPut(t, singles, Item{`pk`: sAttr(`solo`), `note`: sAttr(`hash-only`)})

	nums := db.CreateTable(`Nums`, `pk`, ``, map[string]string{`pk`: `N`})
	mustPut(t, nums, Item{`pk`: nAttr(`1`), `note`: sAttr(`numeric`)})

	bins := db.CreateTable(`Bins`, `pk`, ``, map[string]string{`pk`: `B`})
	mustPut(t, bins, Item{`pk`: bAttr([]byte{1, 2, 3}), `note`: sAttr(`binary`)})

	return db
}

func mustPut(t *testing.T, tb *Table, item Item) {
	t.Helper()
	if err := tb.Put(item); err != nil {
		t.Fatalf(`Put failed: %v`, err)
	}
}

type wireResult struct {
	status  int
	errType string
	raw     string
	fields  map[string]interface{}
}

func call(db *DB, body string) wireResult {
	status, errType, payload := processGetItem(db, []byte(body))
	res := wireResult{status: status, errType: errType, raw: string(payload)}
	var fields map[string]interface{}
	if err := json.Unmarshal(payload, &fields); err == nil {
		res.fields = fields
	}
	return res
}

func expectValidation(t *testing.T, res wireResult, want string) {
	t.Helper()
	if res.status != 400 || res.errType != `ValidationException` {
		t.Fatalf(`expected 400/ValidationException, got status=%d errType=%q body=%s`, res.status, res.errType, res.raw)
	}
	if !strings.Contains(res.raw, want) {
		t.Fatalf(`expected body %s to contain %q`, res.raw, want)
	}
}

func TestGetItemReturnsItem(t *testing.T) {
	db := fixture(t)
	res := call(db, `{"TableName":"Music","Key":{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Promo"}}}`)
	if res.status != 200 || res.errType != `` {
		t.Fatalf(`status=%d errType=%q body=%s`, res.status, res.errType, res.raw)
	}
	want := map[string]interface{}{
		`Item`: map[string]interface{}{
			`Artist`:    map[string]interface{}{`S`: `The Acme Band`},
			`SongTitle`: map[string]interface{}{`S`: `Promo`},
		},
	}
	if !reflect.DeepEqual(res.fields, want) {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemAttributeValueShapes(t *testing.T) {
	db := fixture(t)
	res := call(db, `{"TableName":"Music","Key":{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Look Out, World"}},"ConsistentRead":true}`)
	if res.status != 200 || res.errType != `` {
		t.Fatalf(`status=%d errType=%q body=%s`, res.status, res.errType, res.raw)
	}
	item, ok := res.fields[`Item`].(map[string]interface{})
	if !ok {
		t.Fatalf(`missing Item: %s`, res.raw)
	}
	checks := map[string]interface{}{
		`Artist`:     map[string]interface{}{`S`: `The Acme Band`},
		`AlbumTitle`: map[string]interface{}{`S`: `Songs About Bees`},
		`Year`:       map[string]interface{}{`N`: `1984`},
		`Price`:      map[string]interface{}{`N`: `1.99`},
		`Genre`:      map[string]interface{}{`M`: map[string]interface{}{`Primary`: map[string]interface{}{`S`: `Rock`}, `Styles`: map[string]interface{}{`SS`: []interface{}{`Pop`, `Punk`}}}},
		`Tags`:       map[string]interface{}{`L`: []interface{}{map[string]interface{}{`S`: `first`}, map[string]interface{}{`S`: `second`}}},
		`Blob`:       map[string]interface{}{`B`: `AQI=`},
		`Bare`:       map[string]interface{}{`NULL`: true},
		`Flag`:       map[string]interface{}{`BOOL`: true},
	}
	for k, want := range checks {
		if !reflect.DeepEqual(item[k], want) {
			t.Fatalf(`field %s = %v, want %v`, k, item[k], want)
		}
	}
	if _, ok := res.fields[`ConsumedCapacity`]; ok {
		t.Fatalf(`ConsumedCapacity must be omitted unless requested`)
	}
}

func TestGetItemAbsentOmitsItem(t *testing.T) {
	db := fixture(t)
	res := call(db, `{"TableName":"Music","Key":{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Missing Song"}}}`)
	if res.status != 200 || res.errType != `` || res.raw != `{}` {
		t.Fatalf(`status=%d errType=%q body=%s`, res.status, res.errType, res.raw)
	}
}

func TestGetItemConsumedCapacity(t *testing.T) {
	db := fixture(t)
	key := `{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Promo"}}`

	res := call(db, `{"TableName":"Music","Key":`+key+`,"ReturnConsumedCapacity":"TOTAL"}`)
	want := map[string]interface{}{`TableName`: `Music`, `CapacityUnits`: float64(0.5)}
	if !reflect.DeepEqual(res.fields[`ConsumedCapacity`], want) {
		t.Fatalf(`eventually consistent: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ConsistentRead":true,"ReturnConsumedCapacity":"TOTAL"}`)
	want = map[string]interface{}{`TableName`: `Music`, `CapacityUnits`: float64(1)}
	if !reflect.DeepEqual(res.fields[`ConsumedCapacity`], want) {
		t.Fatalf(`strongly consistent: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ReturnConsumedCapacity":"INDEXES"}`)
	want = map[string]interface{}{`TableName`: `Music`, `CapacityUnits`: float64(0.5)}
	if !reflect.DeepEqual(res.fields[`ConsumedCapacity`], want) {
		t.Fatalf(`INDEXES: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Singles","Key":{"pk":{"S":"ghost"}},"ReturnConsumedCapacity":"TOTAL"}`)
	if _, ok := res.fields[`Item`]; ok {
		t.Fatalf(`absent reads must omit Item: %s`, res.raw)
	}
	want = map[string]interface{}{`TableName`: `Singles`, `CapacityUnits`: float64(0.5)}
	if !reflect.DeepEqual(res.fields[`ConsumedCapacity`], want) {
		t.Fatalf(`absent read capacity: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ConsistentRead":true}`)
	if _, ok := res.fields[`ConsumedCapacity`]; ok {
		t.Fatalf(`default capacity mode must omit the block: %s`, res.raw)
	}
}

func TestGetItemProjection(t *testing.T) {
	db := fixture(t)
	key := `{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Look Out, World"}}`

	res := call(db, `{"TableName":"Music","Key":`+key+`,"ProjectionExpression":"AlbumTitle, Year, Genre.Primary, Tags[1]"}`)
	want := map[string]interface{}{
		`AlbumTitle`: map[string]interface{}{`S`: `Songs About Bees`},
		`Year`:       map[string]interface{}{`N`: `1984`},
		`Genre`:      map[string]interface{}{`M`: map[string]interface{}{`Primary`: map[string]interface{}{`S`: `Rock`}}},
		`Tags`:       map[string]interface{}{`L`: []interface{}{map[string]interface{}{`NULL`: true}, map[string]interface{}{`S`: `second`}}},
	}
	if !reflect.DeepEqual(res.fields[`Item`], want) {
		t.Fatalf(`nested projection: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ProjectionExpression":"#a, Tags[0]","ExpressionAttributeNames":{"#a":"AlbumTitle"}}`)
	want = map[string]interface{}{
		`AlbumTitle`: map[string]interface{}{`S`: `Songs About Bees`},
		`Tags`:       map[string]interface{}{`L`: []interface{}{map[string]interface{}{`S`: `first`}}},
	}
	if !reflect.DeepEqual(res.fields[`Item`], want) {
		t.Fatalf(`placeholder projection: got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ProjectionExpression":"NoSuchAttribute"}`)
	if res.status != 200 {
		t.Fatalf(`status=%d body=%s`, res.status, res.raw)
	}
	if !reflect.DeepEqual(res.fields[`Item`], map[string]interface{}{}) {
		t.Fatalf(`item present but empty projection expected, got %s`, res.raw)
	}

	res = call(db, `{"TableName":"Music","Key":`+key+`,"ProjectionExpression":"Artist, SongTitle"}`)
	want = map[string]interface{}{
		`Artist`:    map[string]interface{}{`S`: `The Acme Band`},
		`SongTitle`: map[string]interface{}{`S`: `Look Out, World`},
	}
	if !reflect.DeepEqual(res.fields[`Item`], want) {
		t.Fatalf(`key attrs projection: got %s`, res.raw)
	}
}

func TestGetItemAttributesToGet(t *testing.T) {
	db := fixture(t)
	key := `{"Artist":{"S":"The Acme Band"},"SongTitle":{"S":"Look Out, World"}}`
	res := call(db, `{"TableName":"Music","Key":`+key+`,"AttributesToGet":["AlbumTitle","NoSuchAttribute"]}`)
	want := map[string]interface{}{
		`AlbumTitle`: map[string]interface{}{`S`: `Songs About Bees`},
	}
	if !reflect.DeepEqual(res.fields[`Item`], want) {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemKeyValidation(t *testing.T) {
	db := fixture(t)
	cases := []struct {
		name string
		body string
		want string
	}{
		{`missing sort key`, `{"TableName":"Music","Key":{"Artist":{"S":"The Acme Band"}}}`, msgKeySchema},
		{`extra key attribute`, `{"TableName":"Music","Key":{"Artist":{"S":"a"},"SongTitle":{"S":"b"},"Year":{"N":"1999"}}}`, msgKeySchema},
		{`empty key map`, `{"TableName":"Music","Key":{}}`, msgKeySchema},
		{`null key`, `{"TableName":"Music","Key":null}`, `Member must not be null`},
		{`absent key`, `{"TableName":"Music"}`, `Member must not be null`},
		{`wrong scalar type`, `{"TableName":"Music","Key":{"Artist":{"S":"a"},"SongTitle":{"N":"1"}}}`, msgKeySchema},
		{`sort key on hash-only table`, `{"TableName":"Singles","Key":{"pk":{"S":"solo"},"sk":{"S":"x"}}}`, msgKeySchema},
		{`empty string key`, `{"TableName":"Music","Key":{"Artist":{"S":""},"SongTitle":{"S":"b"}}}`, `can not contain an empty string value`},
		{`number not parseable`, `{"TableName":"Nums","Key":{"pk":{"N":"1.2.3"}}}`, msgNumberConversion},
		{`key type vs schema`, `{"TableName":"Nums","Key":{"pk":{"S":"1"}}}`, msgKeySchema},
		{`empty table name`, `{"TableName":"","Key":{}}`, `regular expression pattern`},
		{`missing table name`, `{"Key":{"pk":{"S":"x"}}}`, `Value null at 'tableName'`},
		{`bad consumed capacity`, `{"TableName":"Singles","Key":{"pk":{"S":"solo"}},"ReturnConsumedCapacity":"MAYBE"}`, `Member must satisfy enum value set: [INDEXES, TOTAL, NONE]`},
	}
	for _, tc := range cases {
		res := call(db, tc.body)
		expectValidation(t, res, tc.want)
	}
}

func TestGetItemTableNotFound(t *testing.T) {
	db := fixture(t)
	res := call(db, `{"TableName":"GhostTable","Key":{"pk":{"S":"x"}}}`)
	if res.status != 400 || res.errType != `ResourceNotFoundException` {
		t.Fatalf(`status=%d errType=%q body=%s`, res.status, res.errType, res.raw)
	}
	if !strings.Contains(res.raw, `Requested resource not found: Table: GhostTable not found`) {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemExpressionValidation(t *testing.T) {
	db := fixture(t)
	key := `{"Artist":{"S":"a"},"SongTitle":{"S":"b"}}`
	cases := []struct {
		name string
		body string
		want string
	}{
		{`both forms`, `{"TableName":"Music","Key":` + key + `,"AttributesToGet":["AlbumTitle"],"ProjectionExpression":"AlbumTitle"}`, `Cannot specify both AttributesToGet and ProjectionExpression`},
		{`empty AttributesToGet`, `{"TableName":"Music","Key":` + key + `,"AttributesToGet":[]}`, `List of AttibuteNames is empty`},
		{`duplicate AttributesToGet`, `{"TableName":"Music","Key":` + key + `,"AttributesToGet":["AlbumTitle","AlbumTitle"]}`, `Duplicate attribute names are not allowed`},
		{`names without expression`, `{"TableName":"Music","Key":` + key + `,"ExpressionAttributeNames":{"#a":"AlbumTitle"}}`, `ExpressionAttributeNames can only be provided when expressions are specified`},
		{`empty names`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"AlbumTitle","ExpressionAttributeNames":{}}`, `ExpressionAttributeNames must not be empty`},
		{`empty expression`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":""}`, `Member must have length greater than or equal to 1`},
		{`undefined placeholder`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"#zzz"}`, `An expression attribute name used in ProjectionExpression is not defined`},
		{`unused placeholder`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"AlbumTitle","ExpressionAttributeNames":{"#a":"AlbumTitle"}}`, `unused in expressions: keys: {#a}`},
		{`duplicate paths`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"AlbumTitle, AlbumTitle"}`, `Two document paths overlap with each other`},
		{`overlapping paths`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"Genre, Genre.Primary"}`, `Two document paths overlap with each other`},
		{`syntax error`, `{"TableName":"Music","Key":` + key + `,"ProjectionExpression":"Genre["}`, `Syntax error`},
	}
	for _, tc := range cases {
		res := call(db, tc.body)
		expectValidation(t, res, tc.want)
	}
}

func TestGetItemWireLevelErrors(t *testing.T) {
	db := fixture(t)
	cases := []struct {
		body    string
		errType string
	}{
		{`{`, `SerializationException`},
		{`[1]`, `SerializationException`},
		{`{"TableName":123}`, `SerializationException`},
		{`null`, `ValidationException`},
		{`{"TableName":"Singles","Key":{"pk":{"S":"a","N":"1"}}}`, `ValidationException`},
		{`{"TableName":"Singles","Key":{"pk":{}}}`, `ValidationException`},
		{`{"TableName":"Singles","Key":{"pk":{"X":"a"}}}`, `ValidationException`},
		{`{"TableName":"Singles","Key":{"pk":{"S":4}}}`, `SerializationException`},
		{`{"TableName":"Singles","Key":{"pk":null}}`, `SerializationException`},
		{`{"TableName":"Singles","Key":{"pk":{"B":"not-base64!"}}}`, `SerializationException`},
	}
	for _, tc := range cases {
		res := call(db, tc.body)
		if res.status != 400 || res.errType != tc.errType {
			t.Fatalf(`body %s: status=%d errType=%q raw=%s`, tc.body, res.status, res.errType, res.raw)
		}
	}
	res := call(db, `{`)
	if res.raw != `{"__type":"com.amazon.coral.service#SerializationException"}` {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemNumericKeyCanonicallyEqual(t *testing.T) {
	db := fixture(t)
	for _, n := range []string{`1`, `1.00`, `01`, `1e0`, `10e-1`} {
		res := call(db, `{"TableName":"Nums","Key":{"pk":{"N":"`+n+`"}}}`)
		if res.status != 200 || !strings.Contains(res.raw, `"note"`) {
			t.Fatalf(`key %q: got %s`, n, res.raw)
		}
	}
	res := call(db, `{"TableName":"Nums","Key":{"pk":{"N":"2"}}}`)
	if res.raw != `{}` {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemBinaryKey(t *testing.T) {
	db := fixture(t)
	res := call(db, `{"TableName":"Bins","Key":{"pk":{"B":"AQID"}},"ConsistentRead":true}`)
	if res.status != 200 || !strings.Contains(res.raw, `"note"`) {
		t.Fatalf(`got %s`, res.raw)
	}
	res = call(db, `{"TableName":"Bins","Key":{"pk":{"B":"AAAA"}}}`)
	if res.raw != `{}` {
		t.Fatalf(`got %s`, res.raw)
	}
}

func TestGetItemOverHTTP(t *testing.T) {
	db := fixture(t)
	srv := httptest.NewServer(db)
	defer srv.Close()

	post := func(target, body string) *http.Response {
		req, err := http.NewRequest(`POST`, srv.URL, strings.NewReader(body))
		if err != nil {
			t.Fatalf(`NewRequest: %v`, err)
		}
		req.Header.Set(`X-Amz-Target`, target)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(`Do: %v`, err)
		}
		return resp
	}

	resp := post(`DynamoDB_20120810.GetItem`, `{"TableName":"Singles","Key":{"pk":{"S":"solo"}}}`)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf(`ReadAll: %v`, err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf(`status=%d body=%s`, resp.StatusCode, body)
	}
	if got := resp.Header.Get(`Content-Type`); got != `application/x-amz-json-1.0` {
		t.Fatalf(`content type %q`, got)
	}
	if resp.Header.Get(`x-amzn-RequestId`) == `` {
		t.Fatalf(`missing request id header`)
	}
	if !strings.Contains(string(body), `"Item"`) {
		t.Fatalf(`body %s`, body)
	}

	resp2 := post(`DynamoDB_20120810.Query`, `{}`)
	defer resp2.Body.Close()
	if resp2.StatusCode != 400 || resp2.Header.Get(`x-amzn-ErrorType`) != `UnknownOperationException` {
		t.Fatalf(`status=%d errType=%q`, resp2.StatusCode, resp2.Header.Get(`x-amzn-ErrorType`))
	}
}
