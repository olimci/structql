package lexer

import (
	"unicode"
	"unicode/utf8"

	"github.com/olimci/structql/lexer/token"
)

func New(input string) *Lexer {
	return &Lexer{input: input}
}

type Lexer struct {
	input string
	pos   int
}

func (l *Lexer) Next() token.Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return token.Token{Type: token.EOF}
	}

	start := l.pos
	ch, width := l.current()
	if ch == utf8.RuneError && width == 1 {
		l.pos += width
		return token.Token{Type: token.Illegal, Literal: l.input[start:l.pos], Pos: start}
	}

	switch ch {
	case ',':
		l.pos += width
		return token.Token{Type: token.Comma, Literal: ",", Pos: start}
	case '.':
		l.pos += width
		return token.Token{Type: token.Dot, Literal: ".", Pos: start}
	case '(':
		l.pos += width
		return token.Token{Type: token.LParen, Literal: "(", Pos: start}
	case ')':
		l.pos += width
		return token.Token{Type: token.RParen, Literal: ")", Pos: start}
	case '=':
		l.pos += width
		return token.Token{Type: token.Eq, Literal: "=", Pos: start}
	case '!':
		if l.peek('=') {
			l.pos += width + 1
			return token.Token{Type: token.NEq, Literal: "!=", Pos: start}
		}
		l.pos += width
		return token.Token{Type: token.Illegal, Literal: "!", Pos: start}
	case '<':
		if l.peek('=') {
			l.pos += width + 1
			return token.Token{Type: token.LtE, Literal: "<=", Pos: start}
		}
		l.pos += width
		return token.Token{Type: token.Lt, Literal: "<", Pos: start}
	case '>':
		if l.peek('=') {
			l.pos += width + 1
			return token.Token{Type: token.GtE, Literal: ">=", Pos: start}
		}
		l.pos += width
		return token.Token{Type: token.Gt, Literal: ">", Pos: start}
	case '+':
		l.pos += width
		return token.Token{Type: token.Plus, Literal: "+", Pos: start}
	case '-':
		l.pos += width
		return token.Token{Type: token.Minus, Literal: "-", Pos: start}
	case '*':
		l.pos += width
		return token.Token{Type: token.Star, Literal: "*", Pos: start}
	case '/':
		l.pos += width
		return token.Token{Type: token.Slash, Literal: "/", Pos: start}
	case '\'':
		return l.readString()
	}

	if unicode.IsLetter(ch) || ch == '_' {
		return l.readIdentifierOrKeyword()
	}

	if unicode.IsDigit(ch) {
		return l.readNumber()
	}

	l.pos += width
	return token.Token{Type: token.Illegal, Literal: string(ch), Pos: start}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch, width := l.current()
		if ch == utf8.RuneError && width == 1 {
			return
		}
		if !unicode.IsSpace(ch) {
			return
		}
		l.pos += width
	}
}

func (l *Lexer) current() (rune, int) {
	if l.pos >= len(l.input) {
		return utf8.RuneError, 0
	}
	return utf8.DecodeRuneInString(l.input[l.pos:])
}

func (l *Lexer) peek(expected rune) bool {
	peekPos := l.pos
	_, width := utf8.DecodeRuneInString(l.input[peekPos:])
	if width == 0 {
		return false
	}
	peekPos += width
	if peekPos >= len(l.input) {
		return false
	}
	next, _ := utf8.DecodeRuneInString(l.input[peekPos:])
	return next == expected
}

func (l *Lexer) readString() token.Token {
	start := l.pos
	_, width := l.current()
	l.pos += width // skip opening quote

	var out []rune
	invalidUTF8 := false
	for l.pos < len(l.input) {
		ch, width := l.current()
		if ch == utf8.RuneError && width == 1 {
			invalidUTF8 = true
			l.pos += width
			continue
		}
		if ch == '\'' {
			if l.peek('\'') {
				l.pos += width + width
				if !invalidUTF8 {
					out = append(out, '\'')
				}
				continue
			}
			l.pos += width
			if invalidUTF8 {
				return token.Token{Type: token.Illegal, Literal: "invalid utf-8 in string", Pos: start}
			}
			return token.Token{
				Type:    token.String,
				Literal: string(out),
				Pos:     start,
			}
		}
		if !invalidUTF8 {
			out = append(out, ch)
		}
		l.pos += width
	}

	if invalidUTF8 {
		return token.Token{Type: token.Illegal, Literal: "invalid utf-8 in string", Pos: start}
	}
	return token.Token{Type: token.Illegal, Literal: "unterminated string", Pos: start}
}

func (l *Lexer) readIdentifierOrKeyword() token.Token {
	start := l.pos
	for l.pos < len(l.input) {
		ch, width := l.current()
		if ch == utf8.RuneError && width == 1 {
			break
		}
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			break
		}
		l.pos += width
	}
	lit := l.input[start:l.pos]

	if tok, ok := token.LookupKeyword(lit); ok {
		return token.Token{Type: tok, Literal: string(lit), Pos: start}
	}

	return token.Token{Type: token.Identifier, Literal: string(lit), Pos: start}
}

func (l *Lexer) readNumber() token.Token {
	start := l.pos
	for l.pos < len(l.input) {
		ch, width := l.current()
		if ch == utf8.RuneError && width == 1 {
			break
		}
		if !unicode.IsDigit(ch) {
			break
		}
		l.pos += width
	}
	lit := l.input[start:l.pos]
	return token.Token{Type: token.Number, Literal: string(lit), Pos: start}
}
