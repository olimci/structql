package ast

import "github.com/olimci/structql/lexer/token"

type Identifier struct {
	span Span
	Name string
}

func (i Identifier) Span() Span {
	return i.span
}

func (Identifier) exprNode() {}

func NewIdentifier(span Span, name string) Identifier {
	return Identifier{span: span, Name: name}
}

type QualifiedRef struct {
	span  Span
	Parts []Identifier
}

func (q QualifiedRef) Span() Span {
	return q.span
}

func (QualifiedRef) exprNode() {}

func NewQualifiedRef(span Span, parts []Identifier) QualifiedRef {
	return QualifiedRef{span: span, Parts: parts}
}

type NumberLiteral struct {
	span Span
	Raw  string
}

func (n NumberLiteral) Span() Span {
	return n.span
}

func (NumberLiteral) exprNode() {}

func NewNumberLiteral(span Span, raw string) NumberLiteral {
	return NumberLiteral{span: span, Raw: raw}
}

type StringLiteral struct {
	span  Span
	Value string
}

func (s StringLiteral) Span() Span {
	return s.span
}

func (StringLiteral) exprNode() {}

func NewStringLiteral(span Span, value string) StringLiteral {
	return StringLiteral{span: span, Value: value}
}

type BoolLiteral struct {
	span  Span
	Value bool
}

func (b BoolLiteral) Span() Span {
	return b.span
}

func (BoolLiteral) exprNode() {}

func NewBoolLiteral(span Span, value bool) BoolLiteral {
	return BoolLiteral{span: span, Value: value}
}

type NullLiteral struct {
	span Span
}

func (n NullLiteral) Span() Span {
	return n.span
}

func (NullLiteral) exprNode() {}

func NewNullLiteral(span Span) NullLiteral {
	return NullLiteral{span: span}
}

type UnaryExpr struct {
	span Span
	Op   token.Type
	Expr Expr
}

func (u UnaryExpr) Span() Span {
	return u.span
}

func (UnaryExpr) exprNode() {}

func NewUnaryExpr(span Span, op token.Type, expr Expr) UnaryExpr {
	return UnaryExpr{span: span, Op: op, Expr: expr}
}

type BinaryExpr struct {
	span  Span
	Left  Expr
	Op    token.Type
	Right Expr
}

func (b BinaryExpr) Span() Span {
	return b.span
}

func (BinaryExpr) exprNode() {}

func NewBinaryExpr(span Span, left Expr, op token.Type, right Expr) BinaryExpr {
	return BinaryExpr{span: span, Left: left, Op: op, Right: right}
}

type InExpr struct {
	span    Span
	Left    Expr
	Right   []Expr
	Negated bool
}

func (i InExpr) Span() Span {
	return i.span
}

func (InExpr) exprNode() {}

func NewInExpr(span Span, left Expr, right []Expr, negated bool) InExpr {
	return InExpr{span: span, Left: left, Right: right, Negated: negated}
}

type IsExpr struct {
	span    Span
	Left    Expr
	Right   Expr
	Negated bool
}

func (i IsExpr) Span() Span {
	return i.span
}

func (IsExpr) exprNode() {}

func NewIsExpr(span Span, left Expr, right Expr, negated bool) IsExpr {
	return IsExpr{span: span, Left: left, Right: right, Negated: negated}
}

type CallExpr struct {
	span Span
	Name Identifier
	Args []Expr
}

func (c CallExpr) Span() Span {
	return c.span
}

func (CallExpr) exprNode() {}

func NewCallExpr(span Span, name Identifier, args []Expr) CallExpr {
	return CallExpr{span: span, Name: name, Args: args}
}

type SubqueryExpr struct {
	span  Span
	Query *Query
}

func (s SubqueryExpr) Span() Span {
	return s.span
}

func (SubqueryExpr) exprNode() {}

func NewSubqueryExpr(span Span, query *Query) SubqueryExpr {
	return SubqueryExpr{span: span, Query: query}
}
