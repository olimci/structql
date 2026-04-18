package structql

import (
	"reflect"
	"testing"
	"time"
)

type testUser struct {
	ID      int    `structql:"id"`
	Name    string `structql:"name"`
	Age     int    `structql:"age"`
	Active  bool   `structql:"active"`
	CityID  *int   `structql:"city_id"`
	Secret  string `structql:"-"`
	Visible string `structql:"visible_name"`
}

type testCity struct {
	ID   int    `structql:"id"`
	Name string `structql:"name"`
}

type scannedUser struct {
	UserName string  `structql:"user_name"`
	CityName *string `structql:"city_name"`
}

type nestedCity struct {
	Name string `structql:"name"`
}

type nestedAlias struct {
	Name string `structql:"name"`
}

type nestedUser struct {
	ID    int            `structql:"id"`
	City  nestedCity     `structql:"city"`
	Meta  map[string]any `structql:"meta"`
	Tags  []string       `structql:"tags"`
	Alias *nestedAlias   `structql:"alias"`
}

type timedRow struct {
	ID        int       `structql:"id"`
	CreatedAt time.Time `structql:"created_at"`
}

type tagProfile struct {
	ID   int      `structql:"id"`
	Tags []string `structql:"tags"`
}

func TestBuildTableSchemaAndTags(t *testing.T) {
	t.Parallel()

	cityID := 1
	table, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &cityID, Secret: "x", Visible: "A"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, CityID: nil, Secret: "y", Visible: "B"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	if table.Len() != 2 {
		t.Fatalf("unexpected row count: %d", table.Len())
	}
	schema := table.Schema()
	if len(schema) != 6 {
		t.Fatalf("unexpected schema length: %d", len(schema))
	}
	if schema[0].Name != "id" || schema[1].Name != "name" || schema[4].Name != "city_id" {
		t.Fatalf("unexpected schema names: %#v", schema)
	}
	if !schema[4].Nullable {
		t.Fatalf("expected city_id to be nullable")
	}
	for _, col := range schema {
		if col.Name == "Secret" || col.Name == "secret" {
			t.Fatalf("expected secret field to be omitted: %#v", schema)
		}
	}
}

func TestQueryExecutionJoinFilterOrderLimit(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, CityID: &city2, Visible: "Bob"},
		{ID: 3, Name: "Cara", Age: 40, Active: true, CityID: nil, Visible: "Cara"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}
	cities, err := BuildTable([]testCity{
		{ID: 1, Name: "Edmonton"},
		{ID: 2, Name: "Calgary"},
	})
	if err != nil {
		t.Fatalf("BuildTable cities failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("cities", cities); err != nil {
		t.Fatalf("Register cities failed: %v", err)
	}

	result, err := db.Query("select users.name as user_name, cities.name as city_name from users left join cities on users.city_id = cities.id where users.active = true order by users.age desc limit 2")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Columns) != 2 || result.Columns[0].Name != "user_name" || result.Columns[1].Name != "city_name" {
		t.Fatalf("unexpected result columns: %#v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unexpected row count: %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "Cara" {
		t.Fatalf("unexpected first row user: %#v", result.Rows[0])
	}
	if result.Rows[0][1] != nil {
		t.Fatalf("expected nil city for outer-join miss: %#v", result.Rows[0])
	}
	if got := result.Rows[1][0]; got != "Ada" || result.Rows[1][1] != "Edmonton" {
		t.Fatalf("unexpected second row: %#v", result.Rows[1])
	}
}

func TestOrderByTime(t *testing.T) {
	t.Parallel()

	rows, err := BuildTable([]timedRow{
		{ID: 1, CreatedAt: time.Date(2026, 1, 14, 17, 3, 27, 0, time.UTC)},
		{ID: 2, CreatedAt: time.Date(2026, 4, 14, 18, 48, 24, 0, time.UTC)},
		{ID: 3, CreatedAt: time.Date(2026, 3, 12, 22, 6, 49, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("events", rows); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	desc, err := db.Query("select id from events order by created_at desc")
	if err != nil {
		t.Fatalf("desc query failed: %v", err)
	}
	if got := []any{desc.Rows[0][0], desc.Rows[1][0], desc.Rows[2][0]}; !reflect.DeepEqual(got, []any{2, 3, 1}) {
		t.Fatalf("unexpected desc order: %#v", got)
	}

	asc, err := db.Query("select id from events order by created_at asc")
	if err != nil {
		t.Fatalf("asc query failed: %v", err)
	}
	if got := []any{asc.Rows[0][0], asc.Rows[1][0], asc.Rows[2][0]}; !reflect.DeepEqual(got, []any{1, 3, 2}) {
		t.Fatalf("unexpected asc order: %#v", got)
	}
}

func TestUnnestJoin(t *testing.T) {
	t.Parallel()

	rows, err := BuildTable([]tagProfile{
		{ID: 1, Tags: []string{"vip", "beta"}},
		{ID: 2, Tags: []string{"basic"}},
		{ID: 3, Tags: nil},
		{ID: 4, Tags: []string{}},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("profiles", rows); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select p.id, tag.value from profiles p join unnest(p.tags) tag on true order by p.id asc, tag.value asc")
	if err != nil {
		t.Fatalf("unnest join query failed: %v", err)
	}
	if got := len(result.Rows); got != 3 {
		t.Fatalf("unexpected row count: %d", got)
	}
	if result.Rows[0][0] != 1 || result.Rows[0][1] != "beta" {
		t.Fatalf("unexpected first row: %#v", result.Rows[0])
	}
	if result.Rows[1][0] != 1 || result.Rows[1][1] != "vip" {
		t.Fatalf("unexpected second row: %#v", result.Rows[1])
	}
	if result.Rows[2][0] != 2 || result.Rows[2][1] != "basic" {
		t.Fatalf("unexpected third row: %#v", result.Rows[2])
	}

	grouped, err := db.Query("select tag.value, count(p.id) from profiles p join unnest(p.tags) tag on true group by tag.value order by tag.value asc")
	if err != nil {
		t.Fatalf("unnest group query failed: %v", err)
	}
	if len(grouped.Rows) != 3 {
		t.Fatalf("unexpected grouped rows: %#v", grouped.Rows)
	}
	if grouped.Rows[0][0] != "basic" || grouped.Rows[0][1] != int64(1) {
		t.Fatalf("unexpected grouped first row: %#v", grouped.Rows[0])
	}

	leftJoined, err := db.Query("select p.id, tag.value from profiles p left join unnest(p.tags) tag on true order by p.id asc")
	if err != nil {
		t.Fatalf("unnest left join query failed: %v", err)
	}
	if len(leftJoined.Rows) != 5 {
		t.Fatalf("unexpected left join rows: %#v", leftJoined.Rows)
	}
	if leftJoined.Rows[3][0] != 3 || leftJoined.Rows[3][1] != nil {
		t.Fatalf("expected nil expansion for nil tags: %#v", leftJoined.Rows[3])
	}
	if leftJoined.Rows[4][0] != 4 || leftJoined.Rows[4][1] != nil {
		t.Fatalf("expected nil expansion for empty tags: %#v", leftJoined.Rows[4])
	}
}

func TestUnnestErrors(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if _, err := db.Query("select * from users u join unnest(u.name) part on true"); err == nil {
		t.Fatalf("expected non-slice unnest error")
	}
	if _, err := db.Query("select * from users u right join unnest(?) part on true", []string{"x"}); err == nil {
		t.Fatalf("expected right join lateral unsupported error")
	}
}

func TestSelectWildcard(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, CityID: &city2, Visible: "Bob"},
		{ID: 3, Name: "Cara", Age: 40, Active: true, CityID: nil, Visible: "Cara"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select * from users where active = true order by id asc limit 2")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Columns) != 6 {
		t.Fatalf("unexpected column count: %#v", result.Columns)
	}
	if result.Columns[0].Name != "id" || result.Columns[1].Name != "name" || result.Columns[5].Name != "visible_name" {
		t.Fatalf("unexpected column names: %#v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unexpected row count: %#v", result.Rows)
	}
	if result.Rows[0][0] != 1 || result.Rows[1][0] != 3 {
		t.Fatalf("unexpected wildcard rows: %#v", result.Rows)
	}
}

func TestResultMaps(t *testing.T) {
	t.Parallel()

	city1 := 1
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, CityID: nil, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select id, name, city_id from users order by id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	rows := result.Maps()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if got := rows[0]["id"]; got != 1 {
		t.Fatalf("expected first row id 1, got %#v", got)
	}
	if got := rows[0]["name"]; got != "Ada" {
		t.Fatalf("expected first row name Ada, got %#v", got)
	}
	if got := rows[1]["city_id"]; got != nil {
		t.Fatalf("expected second row city_id nil, got %#v", got)
	}
}

func TestQueryArgsAcrossExpressionsAndLimit(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, CityID: &city2, Visible: "Bob"},
		{ID: 3, Name: "Cara", Age: 40, Active: true, CityID: nil, Visible: "Cara"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}
	cities, err := BuildTable([]testCity{
		{ID: 1, Name: "Edmonton"},
		{ID: 2, Name: "Calgary"},
	})
	if err != nil {
		t.Fatalf("BuildTable cities failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("cities", cities); err != nil {
		t.Fatalf("Register cities failed: %v", err)
	}

	result, err := db.Query(
		"select ?, cities.name from users left join cities on users.city_id = cities.id where users.active = ? and users.age in (?, ?) order by users.age desc limit ?",
		"matched",
		true,
		30,
		40,
		1,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("unexpected row count: %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "matched" {
		t.Fatalf("unexpected bound projection: %#v", result.Rows[0])
	}
	if got := result.Rows[0][1]; got != nil {
		t.Fatalf("unexpected city value: %#v", result.Rows[0])
	}
}

func TestQueryArgsInsideScalarSubquery(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: true, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}

	result, err := db.Query("select name, (select ? from users where id = ?) as marker from users where id = ?", "ok", 1, 1)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("unexpected row count: %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Ada" || result.Rows[0][1] != "ok" {
		t.Fatalf("unexpected row: %#v", result.Rows[0])
	}
}

func TestQueryNamedArgsAcrossExpressionsAndLimit(t *testing.T) {
	t.Parallel()

	city1 := 1
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: true, CityID: nil, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}
	cities, err := BuildTable([]testCity{{ID: 1, Name: "Edmonton"}})
	if err != nil {
		t.Fatalf("BuildTable cities failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("cities", cities); err != nil {
		t.Fatalf("Register cities failed: %v", err)
	}

	result, err := db.Query(
		"select @label, (select @label from users where id = @inner_id) as nested, cities.name from users left join cities on users.city_id = cities.id where users.id = ? and users.age = @age limit @limit",
		1,
		Named("label", "chosen"),
		Named("inner_id", 1),
		Named("age", 30),
		Named("limit", 1),
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("unexpected row count: %d", len(result.Rows))
	}
	if result.Rows[0][0] != "chosen" || result.Rows[0][1] != "chosen" || result.Rows[0][2] != "Edmonton" {
		t.Fatalf("unexpected row: %#v", result.Rows[0])
	}
}

func TestQueryArgsValidationErrors(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"}})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}

	if _, err := db.Query("select name from users where id = ?", 1, 2); err == nil {
		t.Fatalf("expected extra arg error")
	}
	if _, err := db.Query("select name from users where id = ?"); err == nil {
		t.Fatalf("expected missing arg error")
	}
	if _, err := db.Query("select name from users limit ?", "bad"); err == nil {
		t.Fatalf("expected invalid LIMIT arg error")
	}
	if _, err := db.Query("select name from users where id = @id", Named("id", 1), 2); err == nil {
		t.Fatalf("expected positional-after-named error")
	}
	if _, err := db.Query("select name from users where id = @id", Named("id", 1), Named("id", 2)); err == nil {
		t.Fatalf("expected duplicate named arg error")
	}
	if _, err := db.Query("select name from users where id = @id"); err == nil {
		t.Fatalf("expected missing named arg error")
	}
	if _, err := db.Query("select name from users where id = 1", Named("id", 1)); err == nil {
		t.Fatalf("expected unused named arg error")
	}
}

func TestRequiredArgs(t *testing.T) {
	t.Parallel()

	positional, named, err := RequiredArgs("select ?, @label, (select ? from users where id = @id) as nested from users where age in (?, @max_age) order by @label limit @limit")
	if err != nil {
		t.Fatalf("RequiredArgs failed: %v", err)
	}

	if positional != 3 {
		t.Fatalf("unexpected positional count: %d", positional)
	}
	if len(named) != 4 {
		t.Fatalf("unexpected named count: %#v", named)
	}
	want := []string{"label", "id", "max_age", "limit"}
	for i, name := range want {
		if named[i] != name {
			t.Fatalf("unexpected named args: %#v", named)
		}
	}
}

func TestRequiredArgsDeduplicatesNamedArgs(t *testing.T) {
	t.Parallel()

	positional, named, err := RequiredArgs("select @Name, @name from users where id = ? and visible_name = @NAME")
	if err != nil {
		t.Fatalf("RequiredArgs failed: %v", err)
	}

	if positional != 1 {
		t.Fatalf("unexpected positional count: %d", positional)
	}
	if len(named) != 1 || named[0] != "Name" {
		t.Fatalf("unexpected named args: %#v", named)
	}
}

func TestRequiredArgsParseError(t *testing.T) {
	t.Parallel()

	if _, _, err := RequiredArgs("select from users"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestPrepareQueryReusesParsedQuery(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"}})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	first, err := db.Prepare("select name from users where id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	second, err := db.Prepare("select name from users where id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if first != second {
		t.Fatalf("expected identical prepared query pointer from cache")
	}

	result, err := first.Query(db, 1)
	if err != nil {
		t.Fatalf("Prepared query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("unexpected prepared query result: %#v", result.Rows)
	}
}

func TestPreparedQueryValidatesArgs(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"}})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	prepared, err := db.Prepare("select ?, @label from users limit @limit")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	if _, err := prepared.Query(db, 1, Named("label", "x"), Named("limit", 1)); err != nil {
		t.Fatalf("prepared query unexpectedly failed: %v", err)
	}
	if _, err := prepared.Query(db, Named("label", "x"), Named("limit", 1)); err == nil {
		t.Fatalf("expected missing positional arg error")
	}
	if _, err := prepared.Query(db, 1, Named("label", "x")); err == nil {
		t.Fatalf("expected missing named arg error")
	}
	if _, err := prepared.Query(db, 1, Named("label", "x"), Named("limit", 1), Named("extra", true)); err == nil {
		t.Fatalf("expected extra named arg error")
	}
}

func TestQueryCaseInsensitiveResolution(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("Users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select NAME from users where ACTIVE = true")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("unexpected result: %#v", result.Rows)
	}
}

func TestQueryAmbiguousColumnError(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{{ID: 1, Name: "Ada", Age: 30, Visible: "Ada"}})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}
	cities, err := BuildTable([]testCity{{ID: 1, Name: "Edmonton"}})
	if err != nil {
		t.Fatalf("BuildTable cities failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("cities", cities); err != nil {
		t.Fatalf("Register cities failed: %v", err)
	}

	if _, err := db.Query("select id from users, cities"); err == nil {
		t.Fatalf("expected ambiguous column error")
	}
}

func TestResultScanIntoStructSlice(t *testing.T) {
	t.Parallel()

	city1 := 1
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Cara", Age: 40, Active: true, CityID: nil, Visible: "Cara"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}
	cities, err := BuildTable([]testCity{
		{ID: 1, Name: "Edmonton"},
	})
	if err != nil {
		t.Fatalf("BuildTable cities failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("cities", cities); err != nil {
		t.Fatalf("Register cities failed: %v", err)
	}

	result, err := db.Query("select users.name as user_name, cities.name as city_name from users left join cities on users.city_id = cities.id order by users.id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	var out []scannedUser
	if err := result.Scan(&out); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(out) != 2 {
		t.Fatalf("unexpected scan length: %d", len(out))
	}
	if out[0].UserName != "Ada" || out[0].CityName == nil || *out[0].CityName != "Edmonton" {
		t.Fatalf("unexpected first scanned row: %#v", out[0])
	}
	if out[1].UserName != "Cara" || out[1].CityName != nil {
		t.Fatalf("unexpected second scanned row: %#v", out[1])
	}
}

func TestResultScanNullIntoNonPointerFieldFails(t *testing.T) {
	t.Parallel()

	result := &Result{
		Columns: []ResultColumn{{Name: "city_name"}},
		Rows:    []Row{{nil}},
	}

	var out []struct {
		CityName string `structql:"city_name"`
	}
	if err := result.Scan(&out); err == nil {
		t.Fatalf("expected scan error")
	}
}

func TestQueryDerivedTableAndScalarSubquery(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable users failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select t.name, (select age from users where id = 1) as top_age from (select name from users) as t order by t.name asc limit 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
	if result.Rows[0][0] != "Ada" || result.Rows[0][1] != 30 {
		t.Fatalf("unexpected result row: %#v", result.Rows[0])
	}
}

func TestQueryGroupByAggregates(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Alan", Age: 25, Active: true, CityID: &city1, Visible: "Alan"},
		{ID: 3, Name: "Bob", Age: 40, Active: false, CityID: &city2, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select city_id, count(age) as cnt, sum(age) as total_age, avg(age) as avg_age, min(age) as min_age, max(age) as max_age from users group by city_id order by city_id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("unexpected grouped rows: %#v", result.Rows)
	}
	if result.Rows[0][0] != 1 || result.Rows[0][1] != int64(2) || result.Rows[0][2] != int64(55) || result.Rows[0][3] != 27.5 {
		t.Fatalf("unexpected first aggregate row: %#v", result.Rows[0])
	}
	if result.Rows[1][0] != 2 || result.Rows[1][1] != int64(1) || result.Rows[1][5] != 40 {
		t.Fatalf("unexpected second aggregate row: %#v", result.Rows[1])
	}
}

func TestQueryGlobalAggregate(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: false, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select count(age) as cnt, max(age) as max_age from users")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(2) || result.Rows[0][1] != 30 {
		t.Fatalf("unexpected global aggregate result: %#v", result.Rows)
	}
}

func TestAggregateCountStarAndDistinct(t *testing.T) {
	t.Parallel()

	city1 := 1
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Alan", Age: 30, Active: true, CityID: &city1, Visible: "Alan"},
		{ID: 3, Name: "Bob", Age: 25, Active: false, CityID: nil, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select count(*) as total_rows, count(distinct age) as distinct_ages, sum(distinct age) as total_age, avg(distinct age) as avg_age from users")
	if err != nil {
		t.Fatalf("aggregate modifier query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unexpected row count: %#v", result.Rows)
	}
	if result.Rows[0][0] != int64(3) || result.Rows[0][1] != int64(2) || result.Rows[0][2] != int64(55) || result.Rows[0][3] != 27.5 {
		t.Fatalf("unexpected aggregate modifier row: %#v", result.Rows[0])
	}

	grouped, err := db.Query("select city_id, count(*) as cnt, count(distinct age) as age_cnt from users group by city_id order by city_id asc")
	if err != nil {
		t.Fatalf("grouped aggregate modifier query failed: %v", err)
	}
	if len(grouped.Rows) != 2 {
		t.Fatalf("unexpected grouped rows: %#v", grouped.Rows)
	}
	if grouped.Rows[0][0] != 1 || grouped.Rows[0][1] != int64(2) || grouped.Rows[0][2] != int64(1) {
		t.Fatalf("unexpected city aggregate row: %#v", grouped.Rows[0])
	}
	if grouped.Rows[1][0] != nil || grouped.Rows[1][1] != int64(1) || grouped.Rows[1][2] != int64(1) {
		t.Fatalf("unexpected nil city aggregate row: %#v", grouped.Rows[1])
	}
}

func TestAggregateCountStarEmptyInput(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select count(*) as total_rows from users")
	if err != nil {
		t.Fatalf("empty count(*) query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(0) {
		t.Fatalf("unexpected empty count(*) result: %#v", result.Rows)
	}
}

func TestInvalidFunctionModifiers(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if _, err := db.Query("select len(*) from users"); err == nil {
		t.Fatalf("expected scalar function star error")
	}
	if _, err := db.Query("select len(distinct name) from users"); err == nil {
		t.Fatalf("expected scalar function DISTINCT error")
	}
	if _, err := db.Query("select sum(*) from users"); err == nil {
		t.Fatalf("expected non-count aggregate star error")
	}
}

func TestQueryHavingAndDistinct(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Alan", Age: 25, Active: true, CityID: &city1, Visible: "Alan"},
		{ID: 3, Name: "Bob", Age: 40, Active: false, CityID: &city2, Visible: "Bob"},
		{ID: 4, Name: "Beth", Age: 40, Active: true, CityID: &city2, Visible: "Beth"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	having, err := db.Query("select city_id, count(age) as cnt from users group by city_id having count(age) > 1 order by city_id asc")
	if err != nil {
		t.Fatalf("HAVING query failed: %v", err)
	}
	if len(having.Rows) != 2 {
		t.Fatalf("unexpected HAVING rows: %#v", having.Rows)
	}
	if having.Rows[0][0] != 1 || having.Rows[0][1] != int64(2) {
		t.Fatalf("unexpected first HAVING row: %#v", having.Rows[0])
	}
	if having.Rows[1][0] != 2 || having.Rows[1][1] != int64(2) {
		t.Fatalf("unexpected second HAVING row: %#v", having.Rows[1])
	}

	distinct, err := db.Query("select distinct age from users order by age desc limit 2")
	if err != nil {
		t.Fatalf("DISTINCT query failed: %v", err)
	}
	if len(distinct.Rows) != 2 {
		t.Fatalf("unexpected DISTINCT rows: %#v", distinct.Rows)
	}
	if distinct.Rows[0][0] != 40 || distinct.Rows[1][0] != 30 {
		t.Fatalf("unexpected DISTINCT ordering/limit rows: %#v", distinct.Rows)
	}
}

func TestHavingRequiresAggregateContext(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if _, err := db.Query("select name from users having name = 'Ada'"); err == nil {
		t.Fatalf("expected HAVING error without aggregate context")
	}
}

func TestSelectWildcardRejectedInAggregateQuery(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"}})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if _, err := db.Query("select *, count(age) from users group by id"); err == nil {
		t.Fatalf("expected aggregate wildcard error")
	}
}

type countTableColumn struct {
	def   Column
	data  []any
	calls *int
}

func (c countTableColumn) Len() int { return len(c.data) }

func (c countTableColumn) ValueAt(i int) any {
	*c.calls++
	return c.data[i]
}

func (c countTableColumn) Column() Column { return c.def }

func TestCorrelatedScalarSubquery(t *testing.T) {
	t.Parallel()

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Alan", Age: 25, Active: true, CityID: &city1, Visible: "Alan"},
		{ID: 3, Name: "Bob", Age: 40, Active: false, CityID: &city2, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select u.name, (select max(age) from users where city_id = u.city_id) as city_max from users u order by u.id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("unexpected row count: %#v", result.Rows)
	}
	if result.Rows[0][1] != 30 || result.Rows[1][1] != 30 || result.Rows[2][1] != 40 {
		t.Fatalf("unexpected correlated results: %#v", result.Rows)
	}
}

func TestCorrelatedScalarSubqueryMemoizesByOuterKey(t *testing.T) {
	t.Parallel()

	calls := 0
	lookup := &Table{
		schema: []Column{
			{Name: "city_id", Type: nil},
			{Name: "age_limit", Type: nil},
		},
		columns: []tableColumn{
			countTableColumn{def: Column{Name: "city_id"}, data: []any{1, 2}, calls: &calls},
			countTableColumn{def: Column{Name: "age_limit"}, data: []any{30, 40}, calls: &calls},
		},
		rows: 2,
	}

	city1 := 1
	city2 := 2
	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, CityID: &city1, Visible: "Ada"},
		{ID: 2, Name: "Alan", Age: 25, Active: true, CityID: &city1, Visible: "Alan"},
		{ID: 3, Name: "Bob", Age: 40, Active: false, CityID: &city2, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}
	if err := db.Register("limits", lookup); err != nil {
		t.Fatalf("Register limits failed: %v", err)
	}

	result, err := db.Query("select u.name, (select age_limit from limits where city_id = u.city_id) as limit_age from users u order by u.id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
	if calls != 4 {
		t.Fatalf("expected correlated subquery memoization by key, got %d column reads", calls)
	}
}

func TestRegisterFunctionAndBuiltins(t *testing.T) {
	t.Parallel()

	users, err := BuildTable([]testUser{
		{ID: 1, Name: "Ada", Age: 30, Active: true, Visible: "Ada"},
		{ID: 2, Name: "Bob", Age: 25, Active: true, Visible: "Bob"},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("users", users); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if err := db.RegisterFunction("prefix", ScalarFunction{
		MinArgs:    1,
		MaxArgs:    1,
		ResultType: reflect.TypeFor[string](),
		Nullable:   true,
		Eval: func(args []any) (any, error) {
			if args[0] == nil {
				return nil, nil
			}
			return "x-" + args[0].(string), nil
		},
	}); err != nil {
		t.Fatalf("RegisterFunction failed: %v", err)
	}

	result, err := db.Query("select prefix(name), len(name), contains(name, 'd') from users where id = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "x-Ada" || result.Rows[0][1] != int64(3) || result.Rows[0][2] != true {
		t.Fatalf("unexpected scalar function result: %#v", result.Rows)
	}

	grouped, err := db.Query("select len(name) as name_len, count(age) from users group by name order by name_len asc")
	if err != nil {
		t.Fatalf("aggregate query failed: %v", err)
	}
	if len(grouped.Rows) != 2 || grouped.Rows[0][0] != int64(3) || grouped.Rows[1][0] != int64(3) {
		t.Fatalf("unexpected aggregate scalar function rows: %#v", grouped.Rows)
	}

	if _, err := db.Query("select missing_fn(name) from users"); err == nil {
		t.Fatalf("expected unknown function error")
	}
	if _, err := db.Query("select len(name, age) from users"); err == nil {
		t.Fatalf("expected arity error")
	}
	if err := db.RegisterFunction("count", ScalarFunction{MinArgs: 1, MaxArgs: 1, Eval: func(args []any) (any, error) { return nil, nil }}); err == nil {
		t.Fatalf("expected reserved-name registration error")
	}
}

func TestNestedFieldAccessAndBuiltins(t *testing.T) {
	t.Parallel()

	rows, err := BuildTable([]nestedUser{
		{
			ID:   1,
			City: nestedCity{Name: "Edmonton"},
			Meta: map[string]any{
				"region": "AB",
				"prefs":  map[string]any{"tier": "gold"},
			},
			Tags:  []string{"vip", "beta"},
			Alias: &nestedAlias{Name: "Ace"},
		},
		{
			ID:    2,
			City:  nestedCity{Name: "Calgary"},
			Meta:  map[string]any{"region": "AB"},
			Tags:  []string{"basic"},
			Alias: nil,
		},
	})
	if err != nil {
		t.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("profiles", rows); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select p.city.name, p.meta.region, p.meta.prefs.tier, len(p.tags), contains(p.tags, 'vip'), p.alias.name from profiles p order by p.id asc")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unexpected row count: %#v", result.Rows)
	}
	if result.Rows[0][0] != "Edmonton" || result.Rows[0][1] != "AB" || result.Rows[0][2] != "gold" || result.Rows[0][3] != int64(2) || result.Rows[0][4] != true || result.Rows[0][5] != "Ace" {
		t.Fatalf("unexpected first nested row: %#v", result.Rows[0])
	}
	if result.Rows[1][2] != nil || result.Rows[1][5] != nil {
		t.Fatalf("expected nil nested values on missing map key / nil pointer: %#v", result.Rows[1])
	}

	if _, err := db.Query("select city.zip from profiles"); err == nil {
		t.Fatalf("expected missing struct field error")
	}
}

func TestBuildMapTableAndNestedQueries(t *testing.T) {
	t.Parallel()

	table, err := BuildMapTable([]map[string]any{
		{
			"Name":  "Ada",
			"Stats": map[string]any{"score": 9},
			"Tags":  []string{"vip", "beta"},
		},
		{
			"name":  "Bob",
			"Stats": map[string]any{"score": 7},
			"Tags":  []string{"basic"},
			"Misc":  nil,
		},
		{
			"stats": map[string]any{"score": 4},
			"misc":  "present",
		},
	})
	if err != nil {
		t.Fatalf("BuildMapTable failed: %v", err)
	}

	schema := table.Schema()
	if len(schema) != 4 {
		t.Fatalf("unexpected schema length: %#v", schema)
	}
	if schema[0].Name != "Misc" || schema[1].Name != "Name" || schema[2].Name != "Stats" || schema[3].Name != "Tags" {
		t.Fatalf("unexpected schema order: %#v", schema)
	}
	if !schema[0].Nullable || !schema[1].Nullable || !schema[3].Nullable {
		t.Fatalf("expected missing-key columns to be nullable: %#v", schema)
	}

	db := NewDB()
	if err := db.Register("people", table); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	result, err := db.Query("select name, stats.score, len(tags), contains(tags, 'vip'), misc from people order by stats.score desc limit 2")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unexpected map query row count: %#v", result.Rows)
	}
	if result.Rows[0][0] != "Ada" || result.Rows[0][1] != 9 || result.Rows[0][2] != int64(2) || result.Rows[0][3] != true {
		t.Fatalf("unexpected first map row: %#v", result.Rows[0])
	}
	if result.Rows[1][0] != "Bob" || result.Rows[1][4] != nil {
		t.Fatalf("unexpected second map row: %#v", result.Rows[1])
	}

	mixed, err := BuildMapTable([]map[string]any{
		{"value": 1},
		{"VALUE": "x"},
	})
	if err != nil {
		t.Fatalf("BuildMapTable mixed failed: %v", err)
	}
	if got := mixed.Schema()[0].Type; got != nil {
		t.Fatalf("expected heterogeneous column type to be nil, got %v", got)
	}
}
