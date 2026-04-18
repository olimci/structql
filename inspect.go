package structql

import (
	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/parser"
)

func RequiredArgs(query string) (int, []string, error) {
	p := parser.New(query)
	parsed, err := p.ParseQuery()
	if err != nil {
		return 0, nil, err
	}

	positional, named := requiredArgsFromAST(parsed)
	return positional, named, nil
}

func requiredArgsFromAST(parsed *ast.Query) (int, []string) {
	if parsed == nil {
		return 0, nil
	}

	maxPositional := -1
	named := make([]string, 0)
	seenNamed := make(map[string]struct{})

	var visitExpr func(ast.Expr)
	var visitQuery func(*ast.Query)
	var visitTableRef func(ast.TableRef)

	visitExpr = func(expr ast.Expr) {
		switch expr := expr.(type) {
		case nil:
			return
		case ast.PlaceholderExpr:
			if expr.Index > maxPositional {
				maxPositional = expr.Index
			}
		case ast.NamedPlaceholderExpr:
			key := normalizeName(expr.Name)
			if _, exists := seenNamed[key]; exists {
				return
			}
			seenNamed[key] = struct{}{}
			named = append(named, expr.Name)
		case ast.UnaryExpr:
			visitExpr(expr.Expr)
		case ast.BinaryExpr:
			visitExpr(expr.Left)
			visitExpr(expr.Right)
		case ast.InExpr:
			visitExpr(expr.Left)
			for _, item := range expr.Right {
				visitExpr(item)
			}
		case ast.IsExpr:
			visitExpr(expr.Left)
			visitExpr(expr.Right)
		case ast.CallExpr:
			for _, arg := range expr.Args {
				visitExpr(arg)
			}
		case ast.SubqueryExpr:
			visitQuery(expr.Query)
		}
	}

	visitTableRef = func(ref ast.TableRef) {
		if ref.Subquery != nil {
			visitQuery(ref.Subquery)
		}
		if ref.Function != nil {
			for _, arg := range ref.Function.Args {
				visitExpr(arg)
			}
		}
	}

	visitQuery = func(query *ast.Query) {
		if query == nil {
			return
		}
		for _, item := range query.Select {
			visitExpr(item.Expr)
		}
		for _, ref := range query.From {
			visitTableRef(ref)
		}
		for _, join := range query.Joins {
			visitTableRef(join.Table)
			visitExpr(join.On)
		}
		visitExpr(query.Where)
		for _, expr := range query.GroupBy {
			visitExpr(expr)
		}
		visitExpr(query.Having)
		for _, term := range query.OrderBy {
			visitExpr(term.Expr)
		}
		visitExpr(query.Limit)
	}

	visitQuery(parsed)

	return maxPositional + 1, named
}
