package structql

import (
	"fmt"
	"slices"

	"github.com/olimci/structql/ast"
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

func (db *DB) Query(query string, args ...any) (*Result, error) {
	prepared, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return prepared.Query(db, args...)
}

func (db *DB) queryAST(q *ast.Query, args []any) (*Result, error) {
	parsedArgs, err := parseQueryArgs(args)
	if err != nil {
		return nil, err
	}
	return db.queryPrepared(&PreparedQuery{ast: q}, parsedArgs)
}

func (db *DB) queryPrepared(prepared *PreparedQuery, parsedArgs *queryArgs) (*Result, error) {
	plan, err := planQueryWithParsedArgs(db, prepared.ast, parsedArgs)
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
		out.Rows[i] = slices.Clone(row)
	}
	return out, nil
}
