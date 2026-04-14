package parser

import (
	"fmt"
	"strings"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/lexer/token"
)

type ParseError struct {
	Message  string
	Span     ast.Span
	Expected []string
	Found    token.Token
}

func (e ParseError) Error() string {
	if len(e.Expected) == 0 {
		return fmt.Sprintf("%s at %d", e.Message, e.Span.Start)
	}

	return fmt.Sprintf("%s at %d: expected %s", e.Message, e.Span.Start, strings.Join(e.Expected, ", "))
}
