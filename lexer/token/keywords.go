package token

import "strings"

var Keywords = map[string]Type{
	"SELECT": Select,
	"FROM":   From,
	"WHERE":  Where,
	"JOIN":   Join,
	"LEFT":   Left,
	"RIGHT":  Right,
	"INNER":  Inner,
	"ON":     On,
	"AS":     As,
	"ORDER":  Order,
	"GROUP":  Group,
	"BY":     By,
	"LIMIT":  Limit,
	"ASC":    Asc,
	"DESC":   Desc,
	"AND":    And,
	"OR":     Or,
	"NOT":    Not,
	"IN":     In,
	"IS":     Is,
	"NULL":   Null,
	"TRUE":   True,
	"FALSE":  False,
}

func LookupKeyword(lit string) (Type, bool) {
	tok, ok := Keywords[strings.ToUpper(lit)]
	return tok, ok
}
