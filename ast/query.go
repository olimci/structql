package ast

type Query struct {
	span    Span
	Select  []SelectItem
	From    []TableRef
	Joins   []Join
	Where   Expr
	GroupBy []Expr
	OrderBy []OrderTerm
	Limit   Expr
}

func (q Query) Span() Span {
	return q.span
}

func (Query) statementNode() {}

func NewQuery(span Span) *Query {
	return &Query{span: span}
}

func (q *Query) SetSpan(span Span) {
	q.span = span
}

type SelectItem struct {
	span  Span
	Expr  Expr
	Alias *Identifier
}

func (s SelectItem) Span() Span {
	return s.span
}

func NewSelectItem(span Span, expr Expr, alias *Identifier) SelectItem {
	return SelectItem{span: span, Expr: expr, Alias: alias}
}

func (s *SelectItem) SetSpan(span Span) {
	s.span = span
}

type TableRef struct {
	span     Span
	Name     *QualifiedRef
	Subquery *Query
	Alias    *Identifier
}

func (t TableRef) Span() Span {
	return t.span
}

func NewNamedTableRef(span Span, name QualifiedRef, alias *Identifier) TableRef {
	return TableRef{span: span, Name: &name, Alias: alias}
}

func NewSubqueryTableRef(span Span, query *Query, alias *Identifier) TableRef {
	return TableRef{span: span, Subquery: query, Alias: alias}
}

func (t *TableRef) SetSpan(span Span) {
	t.span = span
}

type JoinKind uint8

const (
	InnerJoin JoinKind = iota
	LeftJoin
	RightJoin
)

type Join struct {
	span  Span
	Kind  JoinKind
	Table TableRef
	On    Expr
}

func (j Join) Span() Span {
	return j.span
}

func NewJoin(span Span, kind JoinKind, table TableRef, on Expr) Join {
	return Join{span: span, Kind: kind, Table: table, On: on}
}

func (j *Join) SetSpan(span Span) {
	j.span = span
}

type OrderTerm struct {
	span Span
	Expr Expr
	Desc bool
}

func (o OrderTerm) Span() Span {
	return o.span
}

func NewOrderTerm(span Span, expr Expr, desc bool) OrderTerm {
	return OrderTerm{span: span, Expr: expr, Desc: desc}
}

func (o *OrderTerm) SetSpan(span Span) {
	o.span = span
}
