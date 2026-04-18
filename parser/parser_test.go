package parser

import (
	"testing"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/lexer/token"
)

func TestParseQueryFullSurface(t *testing.T) {
	t.Parallel()

	input := "SELECT users.name AS username, age + 1 next_age FROM users u LEFT JOIN profiles p ON u.id = p.user_id WHERE age >= 18 AND active IS NOT NULL ORDER BY users.name DESC, age ASC LIMIT 10"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if len(query.Select) != 2 {
		t.Fatalf("unexpected select item count: %d", len(query.Select))
	}
	if query.Select[0].Alias == nil || query.Select[0].Alias.Name != "username" {
		t.Fatalf("unexpected first alias: %#v", query.Select[0].Alias)
	}
	if _, ok := query.Select[0].Expr.(ast.QualifiedRef); !ok {
		t.Fatalf("expected qualified ref, got %#v", query.Select[0].Expr)
	}

	second, ok := query.Select[1].Expr.(ast.BinaryExpr)
	if !ok || second.Op != token.Plus {
		t.Fatalf("unexpected second select expr: %#v", query.Select[1].Expr)
	}
	if query.Select[1].Alias == nil || query.Select[1].Alias.Name != "next_age" {
		t.Fatalf("unexpected second alias: %#v", query.Select[1].Alias)
	}

	if len(query.From) != 1 || query.From[0].Alias == nil || query.From[0].Alias.Name != "u" {
		t.Fatalf("unexpected from refs: %#v", query.From)
	}
	if len(query.Joins) != 1 || query.Joins[0].Kind != ast.LeftJoin {
		t.Fatalf("unexpected joins: %#v", query.Joins)
	}
	if query.Joins[0].Table.Alias == nil || query.Joins[0].Table.Alias.Name != "p" {
		t.Fatalf("unexpected join alias: %#v", query.Joins[0].Table.Alias)
	}

	where, ok := query.Where.(ast.BinaryExpr)
	if !ok || where.Op != token.And {
		t.Fatalf("unexpected where expr: %#v", query.Where)
	}
	if _, ok := where.Right.(ast.IsExpr); !ok {
		t.Fatalf("expected IS expr on right, got %#v", where.Right)
	}

	if len(query.OrderBy) != 2 || !query.OrderBy[0].Desc || query.OrderBy[1].Desc {
		t.Fatalf("unexpected order by: %#v", query.OrderBy)
	}
	if limit, ok := query.Limit.(ast.NumberLiteral); !ok || limit.Raw != "10" {
		t.Fatalf("unexpected limit: %#v", query.Limit)
	}
	if query.Span().Start != 0 || query.Span().End <= query.Span().Start {
		t.Fatalf("unexpected query span: %#v", query.Span())
	}
}

func TestParseDistinctAndHaving(t *testing.T) {
	t.Parallel()

	input := "SELECT DISTINCT city_id, count(age) AS cnt FROM users GROUP BY city_id HAVING count(age) > 1 ORDER BY city_id ASC LIMIT 5"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if !query.Distinct {
		t.Fatalf("expected DISTINCT flag")
	}
	if len(query.GroupBy) != 1 {
		t.Fatalf("unexpected group by: %#v", query.GroupBy)
	}
	if _, ok := query.Having.(ast.BinaryExpr); !ok {
		t.Fatalf("unexpected having expr: %#v", query.Having)
	}
	if len(query.OrderBy) != 1 || query.OrderBy[0].Desc {
		t.Fatalf("unexpected order by: %#v", query.OrderBy)
	}
	if limit, ok := query.Limit.(ast.NumberLiteral); !ok || limit.Raw != "5" {
		t.Fatalf("unexpected limit: %#v", query.Limit)
	}
}

func TestParseExpressionPrecedence(t *testing.T) {
	t.Parallel()

	input := "SELECT a + b * c FROM t WHERE NOT x = 1 OR y IN (1, 2)"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	selectExpr, ok := query.Select[0].Expr.(ast.BinaryExpr)
	if !ok || selectExpr.Op != token.Plus {
		t.Fatalf("unexpected select expr: %#v", query.Select[0].Expr)
	}
	if right, ok := selectExpr.Right.(ast.BinaryExpr); !ok || right.Op != token.Star {
		t.Fatalf("unexpected multiplication nesting: %#v", selectExpr.Right)
	}

	where, ok := query.Where.(ast.BinaryExpr)
	if !ok || where.Op != token.Or {
		t.Fatalf("unexpected where top-level: %#v", query.Where)
	}
	left, ok := where.Left.(ast.UnaryExpr)
	if !ok || left.Op != token.Not {
		t.Fatalf("unexpected unary left side: %#v", where.Left)
	}
	if _, ok := where.Right.(ast.InExpr); !ok {
		t.Fatalf("unexpected IN expr: %#v", where.Right)
	}
}

func TestParseCallAndNotIn(t *testing.T) {
	t.Parallel()

	input := "SELECT count(name) FROM users WHERE name NOT IN ('a', 'b')"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	call, ok := query.Select[0].Expr.(ast.CallExpr)
	if !ok || call.Name.Name != "count" || len(call.Args) != 1 {
		t.Fatalf("unexpected call expr: %#v", query.Select[0].Expr)
	}
	inExpr, ok := query.Where.(ast.InExpr)
	if !ok || !inExpr.Negated || len(inExpr.Right) != 2 {
		t.Fatalf("unexpected NOT IN expr: %#v", query.Where)
	}
}

func TestParseAggregateCallModifiers(t *testing.T) {
	t.Parallel()

	p := New("SELECT count(*), sum(distinct age) FROM users")
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	countCall, ok := query.Select[0].Expr.(ast.CallExpr)
	if !ok || !countCall.Star || countCall.Distinct || len(countCall.Args) != 0 {
		t.Fatalf("unexpected count(*) call: %#v", query.Select[0].Expr)
	}

	sumCall, ok := query.Select[1].Expr.(ast.CallExpr)
	if !ok || !sumCall.Distinct || sumCall.Star || len(sumCall.Args) != 1 {
		t.Fatalf("unexpected sum(distinct ...) call: %#v", query.Select[1].Expr)
	}
}

func TestParseTableFunction(t *testing.T) {
	t.Parallel()

	input := "SELECT tag.value FROM profiles p JOIN unnest(p.tags) tag ON true"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if len(query.Joins) != 1 {
		t.Fatalf("unexpected joins: %#v", query.Joins)
	}
	if query.Joins[0].Table.Function == nil {
		t.Fatalf("expected table function ref: %#v", query.Joins[0].Table)
	}
	if query.Joins[0].Table.Function.Name.Name != "unnest" {
		t.Fatalf("unexpected table function: %#v", query.Joins[0].Table.Function)
	}
	if query.Joins[0].Table.Alias == nil || query.Joins[0].Table.Alias.Name != "tag" {
		t.Fatalf("unexpected table alias: %#v", query.Joins[0].Table.Alias)
	}
	if len(query.Joins[0].Table.Function.Args) != 1 {
		t.Fatalf("unexpected arg count: %#v", query.Joins[0].Table.Function.Args)
	}
}

func TestParseTableFunctionRequiresAlias(t *testing.T) {
	t.Parallel()

	p := New("SELECT value FROM unnest(tags)")
	query, errs := p.ParseQueryWithErrors()
	if query == nil {
		t.Fatalf("expected partial query")
	}
	if len(errs) == 0 {
		t.Fatalf("expected parse errors")
	}
}

func TestParsePlaceholders(t *testing.T) {
	t.Parallel()

	input := "SELECT ?, @label, (SELECT ? FROM users WHERE id = @id) AS nested FROM users WHERE age IN (?, @max_age) LIMIT @limit"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	first, ok := query.Select[0].Expr.(ast.PlaceholderExpr)
	if !ok || first.Index != 0 {
		t.Fatalf("unexpected first placeholder: %#v", query.Select[0].Expr)
	}
	label, ok := query.Select[1].Expr.(ast.NamedPlaceholderExpr)
	if !ok || label.Name != "label" {
		t.Fatalf("unexpected named placeholder: %#v", query.Select[1].Expr)
	}
	subquery, ok := query.Select[2].Expr.(ast.SubqueryExpr)
	if !ok {
		t.Fatalf("expected subquery expr, got %#v", query.Select[2].Expr)
	}
	nestedSelect, ok := subquery.Query.Select[0].Expr.(ast.PlaceholderExpr)
	if !ok || nestedSelect.Index != 1 {
		t.Fatalf("unexpected nested select placeholder: %#v", subquery.Query.Select[0].Expr)
	}
	nestedWhere, ok := subquery.Query.Where.(ast.BinaryExpr)
	if !ok {
		t.Fatalf("unexpected nested where: %#v", subquery.Query.Where)
	}
	nestedWhereArg, ok := nestedWhere.Right.(ast.NamedPlaceholderExpr)
	if !ok || nestedWhereArg.Name != "id" {
		t.Fatalf("unexpected nested where placeholder: %#v", nestedWhere.Right)
	}
	inExpr, ok := query.Where.(ast.InExpr)
	if !ok || len(inExpr.Right) != 2 {
		t.Fatalf("unexpected IN expr: %#v", query.Where)
	}
	firstIn, ok := inExpr.Right[0].(ast.PlaceholderExpr)
	if !ok || firstIn.Index != 2 {
		t.Fatalf("unexpected first IN placeholder: %#v", inExpr.Right[0])
	}
	secondIn, ok := inExpr.Right[1].(ast.NamedPlaceholderExpr)
	if !ok || secondIn.Name != "max_age" {
		t.Fatalf("unexpected second IN placeholder: %#v", inExpr.Right[1])
	}
	limit, ok := query.Limit.(ast.NamedPlaceholderExpr)
	if !ok || limit.Name != "limit" {
		t.Fatalf("unexpected limit placeholder: %#v", query.Limit)
	}
}

func TestParseSelectWildcard(t *testing.T) {
	t.Parallel()

	p := New("SELECT * FROM users")
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(query.Select) != 1 {
		t.Fatalf("unexpected select count: %d", len(query.Select))
	}
	if query.Select[0].Wildcard == nil {
		t.Fatalf("expected wildcard select item: %#v", query.Select[0])
	}
}

func TestParseQueryErrors(t *testing.T) {
	t.Parallel()

	p := New("SELECT FROM users ORDER name")
	query, errs := p.ParseQueryWithErrors()
	if query == nil {
		t.Fatalf("expected partial query")
	}
	if len(errs) == 0 {
		t.Fatalf("expected parse errors")
	}
	if errs[0].Message != "expected expression in SELECT list" {
		t.Fatalf("unexpected first error: %#v", errs[0])
	}
}

func TestParseUnicodeQuery(t *testing.T) {
	t.Parallel()

	input := "SELECT 名称 FROM café WHERE 名称 IS NOT NULL ORDER BY 名称 DESC LIMIT 5"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if ref, ok := query.Select[0].Expr.(ast.Identifier); !ok || ref.Name != "名称" {
		t.Fatalf("unexpected unicode select expr: %#v", query.Select[0].Expr)
	}
	if query.From[0].Name == nil || query.From[0].Name.Parts[0].Name != "café" {
		t.Fatalf("unexpected unicode table name: %#v", query.From[0])
	}
	if query.OrderBy[0].Span().End <= query.OrderBy[0].Span().Start {
		t.Fatalf("unexpected order span: %#v", query.OrderBy[0].Span())
	}
}

func TestParseMixedCaseSQLPreservesIdentifierSpelling(t *testing.T) {
	t.Parallel()

	input := "select FooBar as MixedAlias from CaféTable where IsActive is not null order by FooBar desc limit 5"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	ident, ok := query.Select[0].Expr.(ast.Identifier)
	if !ok || ident.Name != "FooBar" {
		t.Fatalf("unexpected select identifier: %#v", query.Select[0].Expr)
	}
	if query.Select[0].Alias == nil || query.Select[0].Alias.Name != "MixedAlias" {
		t.Fatalf("unexpected alias: %#v", query.Select[0].Alias)
	}
	if query.From[0].Name == nil {
		t.Fatalf("expected named table ref")
	}
	if got := query.From[0].Name.Parts[0].Name; got != "CaféTable" {
		t.Fatalf("unexpected table identifier: %q", got)
	}
	if _, ok := query.Where.(ast.IsExpr); !ok {
		t.Fatalf("unexpected where expression: %#v", query.Where)
	}
}

func TestParseGroupByAndDerivedTableAndScalarSubquery(t *testing.T) {
	t.Parallel()

	input := "SELECT t.name, (SELECT max(score) FROM scores) AS top_score FROM (SELECT name FROM users) AS t GROUP BY t.name ORDER BY t.name"

	p := New(input)
	query, err := p.ParseQuery()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if len(query.GroupBy) != 1 {
		t.Fatalf("unexpected group by: %#v", query.GroupBy)
	}
	if query.From[0].Subquery == nil || query.From[0].Alias == nil || query.From[0].Alias.Name != "t" {
		t.Fatalf("unexpected derived table: %#v", query.From[0])
	}
	item, ok := query.Select[1].Expr.(ast.SubqueryExpr)
	if !ok || item.Query == nil {
		t.Fatalf("expected scalar subquery expr, got %#v", query.Select[1].Expr)
	}
}
