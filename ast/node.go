package ast

type Node interface {
	Span() Span
}

type Statement interface {
	Node
	statementNode()
}

type Expr interface {
	Node
	exprNode()
}
