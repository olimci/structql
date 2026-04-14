package ast

import (
	"testing"

	"github.com/olimci/structql/lexer/token"
)

func TestQueryImplementsStatement(t *testing.T) {
	t.Parallel()

	var stmt Statement = Query{}
	if got := stmt.Span(); got != (Span{}) {
		t.Fatalf("unexpected zero span: %#v", got)
	}
}

func TestExpressionNodesImplementExpr(t *testing.T) {
	t.Parallel()

	exprs := []Expr{
		Identifier{},
		QualifiedRef{},
		NumberLiteral{},
		StringLiteral{},
		BoolLiteral{},
		NullLiteral{},
		PlaceholderExpr{},
		NamedPlaceholderExpr{},
		UnaryExpr{},
		BinaryExpr{},
		InExpr{},
		IsExpr{},
		CallExpr{},
		SubqueryExpr{},
	}

	for _, expr := range exprs {
		if got := expr.Span(); got != (Span{}) {
			t.Fatalf("unexpected zero span: %#v", got)
		}
	}
}

func TestConstructQueryShape(t *testing.T) {
	t.Parallel()

	col := NewIdentifier(Span{Start: 7, End: 10}, "foo")
	tableName := NewIdentifier(Span{Start: 16, End: 19}, "bar")
	ref := NewQualifiedRef(tableName.Span(), []Identifier{tableName})
	where := NewBinaryExpr(
		Span{Start: 20, End: 33},
		NewIdentifier(Span{Start: 20, End: 23}, "baz"),
		token.Eq,
		NewStringLiteral(Span{Start: 26, End: 33}, "qux"),
	)

	query := NewQuery(Span{Start: 0, End: 33})
	query.Select = []SelectItem{NewSelectItem(col.Span(), col, nil)}
	query.From = []TableRef{NewNamedTableRef(ref.Span(), ref, nil)}
	query.Where = where
	query.OrderBy = []OrderTerm{NewOrderTerm(col.Span(), col, true)}
	query.Limit = NewNumberLiteral(Span{Start: 30, End: 32}, "10")

	if got := query.Span(); got != (Span{Start: 0, End: 33}) {
		t.Fatalf("unexpected query span: %#v", got)
	}
	if query.Select[0].Expr.(Identifier).Name != "foo" {
		t.Fatalf("unexpected select identifier: %#v", query.Select[0].Expr)
	}
	if query.From[0].Name == nil || query.From[0].Name.Parts[0].Name != "bar" {
		t.Fatalf("unexpected table name: %#v", query.From[0].Name)
	}
	if query.Where.(BinaryExpr).Op != token.Eq {
		t.Fatalf("unexpected where operator: %#v", query.Where)
	}
	if !query.OrderBy[0].Desc {
		t.Fatalf("expected descending order term")
	}
	if query.Limit.(NumberLiteral).Raw != "10" {
		t.Fatalf("unexpected limit literal: %#v", query.Limit)
	}
}

func TestMergeSpan(t *testing.T) {
	t.Parallel()

	got := MergeSpan(Span{Start: 2, End: 5}, Span{Start: 8, End: 13})
	want := Span{Start: 2, End: 13}
	if got != want {
		t.Fatalf("got %#v want %#v", got, want)
	}
}
