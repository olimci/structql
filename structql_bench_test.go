package structql

import (
	"fmt"
	"testing"
	"time"
)

type benchPage struct {
	ID       int       `structql:"id"`
	Title    string    `structql:"title"`
	Section  string    `structql:"section"`
	Featured bool      `structql:"featured"`
	Draft    bool      `structql:"draft"`
	Updated  time.Time `structql:"updated"`
	Views    int       `structql:"views"`
}

func benchmarkDB(b testing.TB, rows int) *DB {
	b.Helper()

	pages := make([]benchPage, rows)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range pages {
		pages[i] = benchPage{
			ID:       i + 1,
			Title:    fmt.Sprintf("Page %d", i+1),
			Section:  []string{"posts", "notes", "docs"}[i%3],
			Featured: i%9 == 0,
			Draft:    i%11 == 0,
			Updated:  base.Add(time.Duration(i) * time.Minute),
			Views:    (i * 7) % 1000,
		}
	}

	table, err := BuildTable(pages)
	if err != nil {
		b.Fatalf("BuildTable failed: %v", err)
	}

	db := NewDB()
	if err := db.Register("pages", table); err != nil {
		b.Fatalf("Register failed: %v", err)
	}
	return db
}

func BenchmarkQueryPagesFilterOrderLimit(b *testing.B) {
	db := benchmarkDB(b, 20000)
	query := "select * from pages where featured = true and draft = false and section = 'posts' order by updated desc limit 20"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := db.Query(query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) > 20 {
			b.Fatalf("unexpected row count: %d", len(result.Rows))
		}
	}
}

func BenchmarkPreparedPagesFilterOrderLimit(b *testing.B) {
	db := benchmarkDB(b, 20000)
	query := "select * from pages where featured = true and draft = false and section = 'posts' order by updated desc limit 20"
	prepared, err := db.Prepare(query)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := prepared.Query(db)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) > 20 {
			b.Fatalf("unexpected row count: %d", len(result.Rows))
		}
	}
}

func BenchmarkQueryPagesDistinctSection(b *testing.B) {
	db := benchmarkDB(b, 20000)
	query := "select distinct section from pages order by section asc"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := db.Query(query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			b.Fatalf("expected rows")
		}
	}
}

func BenchmarkQueryPagesGroupedDistinct(b *testing.B) {
	db := benchmarkDB(b, 20000)
	query := "select section, count(distinct views) from pages where draft = false group by section order by section asc"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := db.Query(query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			b.Fatalf("expected rows")
		}
	}
}

func BenchmarkPreparedPagesGroupedDistinct(b *testing.B) {
	db := benchmarkDB(b, 20000)
	query := "select section, count(distinct views) from pages where draft = false group by section order by section asc"
	prepared, err := db.Prepare(query)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := prepared.Query(db)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			b.Fatalf("expected rows")
		}
	}
}
