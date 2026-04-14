package structql

import "testing"

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
	if calls != 8 {
		t.Fatalf("expected correlated subquery memoization by key, got %d column reads", calls)
	}
}
