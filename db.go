package structql

import (
	"fmt"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/parser"
)

func (db *DB) Register(name string, table *Table) error {
	key := normalizeName(name)
	if key == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if _, exists := db.tables[key]; exists {
		return fmt.Errorf("table %q already registered", name)
	}

	db.tables[key] = table

	return nil
}

func (db *DB) Query(query string) (*Result, error) {
	p := parser.New(query)
	parsed, err := p.ParseQuery()
	if err != nil {
		return nil, err
	}
	return db.queryAST(parsed)
}

func (db *DB) queryAST(q *ast.Query) (*Result, error) {
	if db == nil {
		return nil, fmt.Errorf("nil DB")
	}
	if q == nil {
		return nil, fmt.Errorf("nil query")
	}
	plan, err := planQuery(db, q)
	if err != nil {
		return nil, err
	}
	rel, err := plan.execute(nil)
	if err != nil {
		return nil, err
	}

	out := &Result{
		Columns: make([]ResultColumn, len(rel.schema)),
		Rows:    make([]Row, len(rel.rows)),
	}
	for i, col := range rel.schema {
		out.Columns[i] = ResultColumn{Name: col.Name, Type: col.Type, Nullable: col.Nullable}
	}
	for i, row := range rel.rows {
		values := make(Row, len(row))
		copy(values, row)
		out.Rows[i] = values
	}
	return out, nil
}
