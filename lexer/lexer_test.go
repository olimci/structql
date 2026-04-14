package lexer

import (
	"testing"

	"github.com/olimci/structql/lexer/token"
)

func TestLexerScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected []token.Token
	}{
		{
			name:  "ascii query regression",
			input: "SELECT foo, bar FROM baz WHERE qux != 'a''b' AND n <= 10",
			expected: []token.Token{
				{Type: token.Select, Literal: "SELECT", Pos: 0},
				{Type: token.Identifier, Literal: "foo", Pos: 7},
				{Type: token.Comma, Literal: ",", Pos: 10},
				{Type: token.Identifier, Literal: "bar", Pos: 12},
				{Type: token.From, Literal: "FROM", Pos: 16},
				{Type: token.Identifier, Literal: "baz", Pos: 21},
				{Type: token.Where, Literal: "WHERE", Pos: 25},
				{Type: token.Identifier, Literal: "qux", Pos: 31},
				{Type: token.NEq, Literal: "!=", Pos: 35},
				{Type: token.String, Literal: "a'b", Pos: 38},
				{Type: token.And, Literal: "AND", Pos: 45},
				{Type: token.Identifier, Literal: "n", Pos: 49},
				{Type: token.LtE, Literal: "<=", Pos: 51},
				{Type: token.Number, Literal: "10", Pos: 54},
				{Type: token.EOF},
			},
		},
		{
			name:  "unicode identifiers and strings",
			input: "SELECT 名称 FROM café WHERE 名称 = 'naïve ☕'",
			expected: []token.Token{
				{Type: token.Select, Literal: "SELECT", Pos: 0},
				{Type: token.Identifier, Literal: "名称", Pos: 7},
				{Type: token.From, Literal: "FROM", Pos: 14},
				{Type: token.Identifier, Literal: "café", Pos: 19},
				{Type: token.Where, Literal: "WHERE", Pos: 25},
				{Type: token.Identifier, Literal: "名称", Pos: 31},
				{Type: token.Eq, Literal: "=", Pos: 38},
				{Type: token.String, Literal: "naïve ☕", Pos: 40},
				{Type: token.EOF},
			},
		},
		{
			name:  "unicode whitespace",
			input: "\u00a0SELECT\u2003名",
			expected: []token.Token{
				{Type: token.Select, Literal: "SELECT", Pos: 2},
				{Type: token.Identifier, Literal: "名", Pos: 11},
				{Type: token.EOF},
			},
		},
		{
			name:  "delimiters and arithmetic operators",
			input: "(.),?@name+-*/",
			expected: []token.Token{
				{Type: token.LParen, Literal: "(", Pos: 0},
				{Type: token.Dot, Literal: ".", Pos: 1},
				{Type: token.RParen, Literal: ")", Pos: 2},
				{Type: token.Comma, Literal: ",", Pos: 3},
				{Type: token.Question, Literal: "?", Pos: 4},
				{Type: token.NamedArg, Literal: "@name", Pos: 5},
				{Type: token.Plus, Literal: "+", Pos: 10},
				{Type: token.Minus, Literal: "-", Pos: 11},
				{Type: token.Star, Literal: "*", Pos: 12},
				{Type: token.Slash, Literal: "/", Pos: 13},
				{Type: token.EOF},
			},
		},
		{
			name:  "named arg tokenization",
			input: "@org @名_2 @",
			expected: []token.Token{
				{Type: token.NamedArg, Literal: "@org", Pos: 0},
				{Type: token.NamedArg, Literal: "@名_2", Pos: 5},
				{Type: token.Illegal, Literal: "@", Pos: 12},
				{Type: token.EOF},
			},
		},
		{
			name:  "comparison operators",
			input: "= != < <= > >=",
			expected: []token.Token{
				{Type: token.Eq, Literal: "=", Pos: 0},
				{Type: token.NEq, Literal: "!=", Pos: 2},
				{Type: token.Lt, Literal: "<", Pos: 5},
				{Type: token.LtE, Literal: "<=", Pos: 7},
				{Type: token.Gt, Literal: ">", Pos: 10},
				{Type: token.GtE, Literal: ">=", Pos: 12},
				{Type: token.EOF},
			},
		},
		{
			name:  "all keywords stay keywords",
			input: "SELECT FROM WHERE JOIN LEFT RIGHT INNER ON AS ORDER BY LIMIT ASC DESC AND OR NOT IN IS NULL TRUE FALSE",
			expected: []token.Token{
				{Type: token.Select, Literal: "SELECT", Pos: 0},
				{Type: token.From, Literal: "FROM", Pos: 7},
				{Type: token.Where, Literal: "WHERE", Pos: 12},
				{Type: token.Join, Literal: "JOIN", Pos: 18},
				{Type: token.Left, Literal: "LEFT", Pos: 23},
				{Type: token.Right, Literal: "RIGHT", Pos: 28},
				{Type: token.Inner, Literal: "INNER", Pos: 34},
				{Type: token.On, Literal: "ON", Pos: 40},
				{Type: token.As, Literal: "AS", Pos: 43},
				{Type: token.Order, Literal: "ORDER", Pos: 46},
				{Type: token.By, Literal: "BY", Pos: 52},
				{Type: token.Limit, Literal: "LIMIT", Pos: 55},
				{Type: token.Asc, Literal: "ASC", Pos: 61},
				{Type: token.Desc, Literal: "DESC", Pos: 65},
				{Type: token.And, Literal: "AND", Pos: 70},
				{Type: token.Or, Literal: "OR", Pos: 74},
				{Type: token.Not, Literal: "NOT", Pos: 77},
				{Type: token.In, Literal: "IN", Pos: 81},
				{Type: token.Is, Literal: "IS", Pos: 84},
				{Type: token.Null, Literal: "NULL", Pos: 87},
				{Type: token.True, Literal: "TRUE", Pos: 92},
				{Type: token.False, Literal: "FALSE", Pos: 97},
				{Type: token.EOF},
			},
		},
		{
			name:  "mixed case keywords and boolean null literals",
			input: "select fRoM wHeRe TrUe FaLsE nUlL",
			expected: []token.Token{
				{Type: token.Select, Literal: "select", Pos: 0},
				{Type: token.From, Literal: "fRoM", Pos: 7},
				{Type: token.Where, Literal: "wHeRe", Pos: 12},
				{Type: token.True, Literal: "TrUe", Pos: 18},
				{Type: token.False, Literal: "FaLsE", Pos: 23},
				{Type: token.Null, Literal: "nUlL", Pos: 29},
				{Type: token.EOF},
			},
		},
		{
			name:  "keyword boundaries and identifier forms",
			input: "_name SELECTED café_123 名2 SeLeCtEd",
			expected: []token.Token{
				{Type: token.Identifier, Literal: "_name", Pos: 0},
				{Type: token.Identifier, Literal: "SELECTED", Pos: 6},
				{Type: token.Identifier, Literal: "café_123", Pos: 15},
				{Type: token.Identifier, Literal: "名2", Pos: 25},
				{Type: token.Identifier, Literal: "SeLeCtEd", Pos: 30},
				{Type: token.EOF},
			},
		},
		{
			name:  "numbers stop before letters",
			input: "123abc 45名",
			expected: []token.Token{
				{Type: token.Number, Literal: "123", Pos: 0},
				{Type: token.Identifier, Literal: "abc", Pos: 3},
				{Type: token.Number, Literal: "45", Pos: 7},
				{Type: token.Identifier, Literal: "名", Pos: 9},
				{Type: token.EOF},
			},
		},
		{
			name:  "escaped quotes in unicode string",
			input: "'l''été 名'",
			expected: []token.Token{
				{Type: token.String, Literal: "l'été 名", Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "unterminated string",
			input: "'naïve",
			expected: []token.Token{
				{Type: token.Illegal, Literal: "unterminated string", Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "illegal bang",
			input: "!",
			expected: []token.Token{
				{Type: token.Illegal, Literal: "!", Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "invalid utf8 at top level",
			input: string([]byte{0xff}),
			expected: []token.Token{
				{Type: token.Illegal, Literal: string([]byte{0xff}), Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "invalid utf8 inside string",
			input: "'" + string([]byte{0xff}) + "'",
			expected: []token.Token{
				{Type: token.Illegal, Literal: "invalid utf-8 in string", Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "invalid utf8 inside unterminated string",
			input: "'" + string([]byte{0xff}) + "x",
			expected: []token.Token{
				{Type: token.Illegal, Literal: "invalid utf-8 in string", Pos: 0},
				{Type: token.EOF},
			},
		},
		{
			name:  "mixed unicode byte offsets stay byte based",
			input: "名 = '☕'",
			expected: []token.Token{
				{Type: token.Identifier, Literal: "名", Pos: 0},
				{Type: token.Eq, Literal: "=", Pos: 4},
				{Type: token.String, Literal: "☕", Pos: 6},
				{Type: token.EOF},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertTokens(t, tt.input, tt.expected)
		})
	}
}

func assertTokens(t *testing.T, input string, expected []token.Token) {
	t.Helper()

	l := New(input)
	for i, want := range expected {
		got := l.Next()
		if got != want {
			t.Fatalf("token %d: got %#v want %#v", i, got, want)
		}
	}
}
