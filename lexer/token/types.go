package token

type Type uint64

const (
	EOF Type = iota
	Illegal

	Identifier
	Number
	String

	Comma
	Dot
	LParen
	RParen

	Eq
	NEq
	Lt
	LtE
	Gt
	GtE

	Plus
	Minus
	Star
	Slash
	Percent

	Select
	From
	Where
	Join
	Left
	Right
	Inner
	On
	As
	Order
	Group
	By
	Limit
	Asc
	Desc
	And
	Or
	Not
	In
	Is
	Null
	True
	False
)
