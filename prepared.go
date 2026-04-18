package structql

import (
	"fmt"
	"slices"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/parser"
)

type PreparedQuery struct {
	query      string
	ast        *ast.Query
	positional int
	named      []string
}

func (db *DB) Prepare(query string) (*PreparedQuery, error) {
	if db == nil {
		return nil, fmt.Errorf("nil DB")
	}

	db.cacheMu.RLock()
	cached := db.queryCache[query]
	db.cacheMu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	p := parser.New(query)
	parsed, err := p.ParseQuery()
	if err != nil {
		return nil, err
	}

	positional, named := requiredArgsFromAST(parsed)
	prepared := &PreparedQuery{
		query:      query,
		ast:        parsed,
		positional: positional,
		named:      slices.Clone(named),
	}

	db.cacheMu.Lock()
	if cached = db.queryCache[query]; cached == nil {
		db.queryCache[query] = prepared
		cached = prepared
	}
	db.cacheMu.Unlock()

	return cached, nil
}

func (p *PreparedQuery) Query(db *DB, args ...any) (*Result, error) {
	if p == nil {
		return nil, fmt.Errorf("nil prepared query")
	}
	if db == nil {
		return nil, fmt.Errorf("nil DB")
	}
	parsedArgs, err := parseQueryArgs(args)
	if err != nil {
		return nil, err
	}
	if err := parsedArgs.validate(p.positional, p.named); err != nil {
		return nil, err
	}
	return db.queryPrepared(p, parsedArgs)
}
