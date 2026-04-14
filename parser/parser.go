package parser

import (
	"fmt"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/lexer"
	"github.com/olimci/structql/lexer/token"
)

const (
	precLowest int = iota
	precOr
	precAnd
	precCompare
	precAdd
	precMultiply
	precPrefix
)

type Parser struct {
	lexer  *lexer.Lexer
	cur    token.Token
	peek   token.Token
	errors []ParseError
	args   int
}

func New(input string) *Parser {
	l := lexer.New(input)
	p := &Parser{lexer: l}
	p.cur = l.Next()
	p.peek = l.Next()
	return p
}

func (p *Parser) Errors() []ParseError {
	out := make([]ParseError, len(p.errors))
	copy(out, p.errors)
	return out
}

func (p *Parser) ParseQuery() (*ast.Query, error) {
	query, errs := p.ParseQueryWithErrors()
	if len(errs) > 0 {
		return query, errs[0]
	}
	return query, nil
}

func (p *Parser) ParseQueryWithErrors() (*ast.Query, []ParseError) {
	query := p.parseQuery()
	errs := p.Errors()
	if query == nil {
		return nil, errs
	}
	return query, errs
}

func (p *Parser) parseQuery() *ast.Query {
	startTok, ok := p.expect(token.Select, "expected SELECT to start query")
	if !ok {
		p.syncTo(token.Select, token.EOF)
		if p.cur.Type != token.Select {
			return nil
		}
		startTok = p.cur
	}

	query := ast.NewQuery(ast.Span{})
	query.Select = p.parseSelectList()

	fromTok, ok := p.expect(token.From, "expected FROM after SELECT list")
	if !ok {
		query.SetSpan(ast.MergeSpan(spanFromToken(startTok), spanFromToken(p.cur)))
		return query
	}

	query.From = p.parseFromList()

	for p.isJoinStart() {
		join := p.parseJoin()
		if join == nil {
			break
		}
		query.Joins = append(query.Joins, *join)
	}

	if p.cur.Type == token.Where {
		p.advance()
		query.Where = p.parseExpression(precLowest)
		if query.Where == nil {
			p.errorAtCurrent("expected expression after WHERE", nil)
			p.syncTo(token.Group, token.Order, token.Limit, token.EOF)
		}
	}

	if p.cur.Type == token.Group {
		query.GroupBy = p.parseGroupBy()
	}

	if p.cur.Type == token.Order {
		query.OrderBy = p.parseOrderBy()
	}

	if p.cur.Type == token.Limit {
		p.advance()
		query.Limit = p.parseExpression(precLowest)
		if query.Limit == nil {
			p.errorAtCurrent("expected expression after LIMIT", nil)
		}
	}

	last := spanFromToken(fromTok)
	if len(query.Select) > 0 {
		last = query.Select[len(query.Select)-1].Span()
	}
	if len(query.From) > 0 {
		last = query.From[len(query.From)-1].Span()
	}
	if len(query.Joins) > 0 {
		last = query.Joins[len(query.Joins)-1].Span()
	}
	if query.Where != nil {
		last = query.Where.Span()
	}
	if len(query.OrderBy) > 0 {
		last = query.OrderBy[len(query.OrderBy)-1].Span()
	}
	if query.Limit != nil {
		last = query.Limit.Span()
	}
	query.SetSpan(ast.MergeSpan(spanFromToken(startTok), last))
	return query
}

func (p *Parser) parseSelectList() []ast.SelectItem {
	var items []ast.SelectItem
	for {
		item := p.parseSelectItem()
		if item == nil {
			return items
		}
		items = append(items, *item)
		if p.cur.Type != token.Comma {
			return items
		}
		p.advance()
	}
}

func (p *Parser) parseSelectItem() *ast.SelectItem {
	if wildcard := p.parseSelectWildcard(); wildcard != nil {
		if p.cur.Type == token.As || p.cur.Type == token.Identifier {
			p.errorAtCurrent("wildcard SELECT items cannot have aliases", nil)
		}
		return wildcard
	}

	expr := p.parseExpression(precLowest)
	if expr == nil {
		p.errorAtCurrent("expected expression in SELECT list", nil)
		p.syncTo(token.Comma, token.From, token.EOF)
		return nil
	}

	item := ast.NewSelectItem(expr.Span(), expr, nil)

	if p.cur.Type == token.As {
		p.advance()
		alias := p.parseIdentifier()
		if alias == nil {
			p.errorAtCurrent("expected identifier after AS", []string{"identifier"})
			return &item
		}
		item.Alias = alias
		item.SetSpan(ast.MergeSpan(item.Span(), alias.Span()))
		return &item
	}

	if p.cur.Type == token.Identifier {
		alias := p.parseIdentifier()
		item.Alias = alias
		item.SetSpan(ast.MergeSpan(item.Span(), alias.Span()))
	}

	return &item
}

func (p *Parser) parseSelectWildcard() *ast.SelectItem {
	if p.cur.Type == token.Star {
		tok := p.cur
		p.advance()
		item := ast.NewSelectWildcardItem(spanFromToken(tok), nil)
		return &item
	}
	return nil
}

func (p *Parser) parseFromList() []ast.TableRef {
	var refs []ast.TableRef
	for {
		ref := p.parseTableRef()
		if ref == nil {
			p.errorAtCurrent("expected table reference after FROM", nil)
			p.syncTo(token.Where, token.Join, token.Left, token.Right, token.Inner, token.Group, token.Order, token.Limit, token.EOF)
			return refs
		}
		refs = append(refs, *ref)
		if p.cur.Type != token.Comma {
			return refs
		}
		p.advance()
	}
}

func (p *Parser) parseTableRef() *ast.TableRef {
	if p.cur.Type == token.LParen && p.peek.Type == token.Select {
		start := p.cur
		p.advance()
		subquery := p.parseQuery()
		if subquery == nil {
			p.errorAtCurrent("expected subquery after (", nil)
			return nil
		}
		endTok, ok := p.expect(token.RParen, "expected ) after subquery")
		if !ok {
			return nil
		}

		var alias *ast.Identifier
		if p.cur.Type == token.As {
			p.advance()
			alias = p.parseIdentifier()
		} else if p.cur.Type == token.Identifier {
			alias = p.parseIdentifier()
		}
		if alias == nil {
			p.errorAtCurrent("derived table subqueries require an alias", []string{"identifier"})
			return nil
		}

		ref := ast.NewSubqueryTableRef(ast.MergeSpan(spanFromToken(start), spanFromToken(endTok)), subquery, alias)
		ref.SetSpan(ast.MergeSpan(ref.Span(), alias.Span()))
		return &ref
	}

	name := p.parseQualifiedRef()
	if name == nil {
		return nil
	}

	ref := ast.NewNamedTableRef(name.Span(), *name, nil)

	if p.cur.Type == token.As {
		p.advance()
		alias := p.parseIdentifier()
		if alias == nil {
			p.errorAtCurrent("expected identifier after AS", []string{"identifier"})
			return &ref
		}
		ref.Alias = alias
		ref.SetSpan(ast.MergeSpan(ref.Span(), alias.Span()))
		return &ref
	}

	if p.cur.Type == token.Identifier {
		alias := p.parseIdentifier()
		ref.Alias = alias
		ref.SetSpan(ast.MergeSpan(ref.Span(), alias.Span()))
	}

	return &ref
}

func (p *Parser) parseJoin() *ast.Join {
	start := p.cur
	kind := ast.InnerJoin
	switch p.cur.Type {
	case token.Join:
		p.advance()
	case token.Left:
		kind = ast.LeftJoin
		p.advance()
		if _, ok := p.expect(token.Join, "expected JOIN after LEFT"); !ok {
			return nil
		}
	case token.Right:
		kind = ast.RightJoin
		p.advance()
		if _, ok := p.expect(token.Join, "expected JOIN after RIGHT"); !ok {
			return nil
		}
	case token.Inner:
		kind = ast.InnerJoin
		p.advance()
		if _, ok := p.expect(token.Join, "expected JOIN after INNER"); !ok {
			return nil
		}
	default:
		return nil
	}

	ref := p.parseTableRef()
	if ref == nil {
		p.errorAtCurrent("expected table reference after JOIN", nil)
		p.syncTo(token.On, token.Where, token.Group, token.Order, token.Limit, token.EOF)
		return nil
	}

	if _, ok := p.expect(token.On, "expected ON after JOIN table"); !ok {
		join := ast.NewJoin(ast.MergeSpan(spanFromToken(start), ref.Span()), kind, *ref, nil)
		return &join
	}

	on := p.parseExpression(precLowest)
	if on == nil {
		p.errorAtCurrent("expected expression after ON", nil)
		p.syncTo(token.Where, token.Group, token.Order, token.Limit, token.EOF)
		join := ast.NewJoin(ast.MergeSpan(spanFromToken(start), ref.Span()), kind, *ref, nil)
		return &join
	}

	join := ast.NewJoin(ast.MergeSpan(spanFromToken(start), on.Span()), kind, *ref, on)
	return &join
}

func (p *Parser) parseGroupBy() []ast.Expr {
	p.advance()
	if _, ok := p.expect(token.By, "expected BY after GROUP"); !ok {
		return nil
	}

	var exprs []ast.Expr
	for {
		expr := p.parseExpression(precLowest)
		if expr == nil {
			p.errorAtCurrent("expected expression in GROUP BY", nil)
			p.syncTo(token.Comma, token.Order, token.Limit, token.EOF)
			return exprs
		}
		exprs = append(exprs, expr)
		if p.cur.Type != token.Comma {
			return exprs
		}
		p.advance()
	}
}

func (p *Parser) parseOrderBy() []ast.OrderTerm {
	p.advance()
	if _, ok := p.expect(token.By, "expected BY after ORDER"); !ok {
		return nil
	}

	var terms []ast.OrderTerm
	for {
		expr := p.parseExpression(precLowest)
		if expr == nil {
			p.errorAtCurrent("expected expression in ORDER BY", nil)
			p.syncTo(token.Comma, token.Limit, token.EOF)
			return terms
		}

		term := ast.NewOrderTerm(expr.Span(), expr, false)
		if p.cur.Type == token.Asc {
			term.SetSpan(ast.MergeSpan(term.Span(), spanFromToken(p.cur)))
			p.advance()
		} else if p.cur.Type == token.Desc {
			term.Desc = true
			term.SetSpan(ast.MergeSpan(term.Span(), spanFromToken(p.cur)))
			p.advance()
		}

		terms = append(terms, term)
		if p.cur.Type != token.Comma {
			return terms
		}
		p.advance()
	}
}

func (p *Parser) parseExpression(precedence int) ast.Expr {
	left := p.parsePrefix()
	if left == nil {
		return nil
	}

	for !p.expressionTerminated() && precedence < p.currentPrecedence() {
		switch {
		case p.cur.Type == token.Is:
			left = p.parseIsExpr(left)
		case p.cur.Type == token.In:
			left = p.parseInExpr(left, false)
		case p.cur.Type == token.Not && p.peek.Type == token.In:
			left = p.parseNotInExpr(left)
		case isBinaryOperator(p.cur.Type):
			left = p.parseBinaryExpr(left)
		default:
			return left
		}
		if left == nil {
			return nil
		}
	}

	return left
}

func (p *Parser) parsePrefix() ast.Expr {
	switch p.cur.Type {
	case token.Identifier:
		return p.parseIdentifierLike()
	case token.Number:
		tok := p.cur
		p.advance()
		return ast.NewNumberLiteral(spanFromToken(tok), tok.Literal)
	case token.String:
		tok := p.cur
		p.advance()
		return ast.NewStringLiteral(spanFromToken(tok), tok.Literal)
	case token.True:
		tok := p.cur
		p.advance()
		return ast.NewBoolLiteral(spanFromToken(tok), true)
	case token.False:
		tok := p.cur
		p.advance()
		return ast.NewBoolLiteral(spanFromToken(tok), false)
	case token.Null:
		tok := p.cur
		p.advance()
		return ast.NewNullLiteral(spanFromToken(tok))
	case token.Question:
		tok := p.cur
		index := p.args
		p.args++
		p.advance()
		return ast.NewPlaceholderExpr(spanFromToken(tok), index)
	case token.NamedArg:
		tok := p.cur
		p.advance()
		return ast.NewNamedPlaceholderExpr(spanFromToken(tok), tok.Literal[1:])
	case token.Not, token.Minus:
		tok := p.cur
		p.advance()
		rightPrec := precPrefix
		if tok.Type == token.Not {
			rightPrec = precAnd
		}
		right := p.parseExpression(rightPrec)
		if right == nil {
			p.errorAtCurrent(fmt.Sprintf("expected expression after %s", tok.Literal), nil)
			return nil
		}
		return ast.NewUnaryExpr(ast.MergeSpan(spanFromToken(tok), right.Span()), tok.Type, right)
	case token.LParen:
		if p.peek.Type == token.Select {
			start := p.cur
			p.advance()
			query := p.parseQuery()
			if query == nil {
				p.errorAtCurrent("expected subquery after (", nil)
				return nil
			}
			endTok, ok := p.expect(token.RParen, "expected ) after subquery")
			if !ok {
				return nil
			}
			return ast.NewSubqueryExpr(ast.MergeSpan(spanFromToken(start), spanFromToken(endTok)), query)
		}
		p.advance()
		expr := p.parseExpression(precLowest)
		if expr == nil {
			p.errorAtCurrent("expected expression after (", nil)
			return nil
		}
		if _, ok := p.expect(token.RParen, "expected ) to close expression"); !ok {
			return expr
		}
		return expr
	case token.Illegal:
		p.errorAtCurrent("illegal token in expression", nil)
		return nil
	default:
		return nil
	}
}

func (p *Parser) parseIdentifierLike() ast.Expr {
	ident := p.parseIdentifier()
	if ident == nil {
		return nil
	}

	if p.cur.Type == token.LParen {
		return p.parseCallExpr(*ident)
	}

	parts := []ast.Identifier{*ident}
	end := ident.Span()
	for p.cur.Type == token.Dot {
		p.advance()
		next := p.parseIdentifier()
		if next == nil {
			p.errorAtCurrent("expected identifier after .", []string{"identifier"})
			break
		}
		parts = append(parts, *next)
		end = next.Span()
	}

	if len(parts) == 1 {
		return *ident
	}

	return ast.NewQualifiedRef(ast.MergeSpan(ident.Span(), end), parts)
}

func (p *Parser) parseCallExpr(name ast.Identifier) ast.Expr {
	start := name.Span()
	p.advance()

	var args []ast.Expr
	if p.cur.Type != token.RParen {
		for {
			arg := p.parseExpression(precLowest)
			if arg == nil {
				p.errorAtCurrent("expected expression in function call", nil)
				break
			}
			args = append(args, arg)
			if p.cur.Type != token.Comma {
				break
			}
			p.advance()
		}
	}

	end := start
	if closing, ok := p.expect(token.RParen, "expected ) to close function call"); ok {
		end = spanFromToken(closing)
	} else if len(args) > 0 {
		end = args[len(args)-1].Span()
	}

	return ast.NewCallExpr(ast.MergeSpan(start, end), name, args)
}

func (p *Parser) parseBinaryExpr(left ast.Expr) ast.Expr {
	op := p.cur
	prec := p.currentPrecedence()
	p.advance()
	right := p.parseExpression(prec)
	if right == nil {
		p.errorAtCurrent(fmt.Sprintf("expected expression after %s", tokenLabel(op.Type, op.Literal)), nil)
		return left
	}
	return ast.NewBinaryExpr(ast.MergeSpan(left.Span(), right.Span()), left, op.Type, right)
}

func (p *Parser) parseIsExpr(left ast.Expr) ast.Expr {
	start := left.Span()
	p.advance()

	negated := false
	if p.cur.Type == token.Not {
		negated = true
		p.advance()
	}

	right := p.parseExpression(precCompare)
	if right == nil {
		p.errorAtCurrent("expected expression after IS", nil)
		return left
	}

	return ast.NewIsExpr(ast.MergeSpan(start, right.Span()), left, right, negated)
}

func (p *Parser) parseInExpr(left ast.Expr, negated bool) ast.Expr {
	start := left.Span()
	p.advance()

	if _, ok := p.expect(token.LParen, "expected ( after IN"); !ok {
		return left
	}

	var exprs []ast.Expr
	for p.cur.Type != token.RParen && p.cur.Type != token.EOF {
		expr := p.parseExpression(precLowest)
		if expr == nil {
			p.errorAtCurrent("expected expression in IN list", nil)
			break
		}
		exprs = append(exprs, expr)
		if p.cur.Type != token.Comma {
			break
		}
		p.advance()
	}

	end := start
	if closing, ok := p.expect(token.RParen, "expected ) to close IN list"); ok {
		end = spanFromToken(closing)
	} else if len(exprs) > 0 {
		end = exprs[len(exprs)-1].Span()
	}

	return ast.NewInExpr(ast.MergeSpan(start, end), left, exprs, negated)
}

func (p *Parser) parseNotInExpr(left ast.Expr) ast.Expr {
	p.advance()
	if p.cur.Type != token.In {
		return left
	}
	return p.parseInExpr(left, true)
}

func (p *Parser) parseIdentifier() *ast.Identifier {
	if p.cur.Type != token.Identifier {
		return nil
	}
	tok := p.cur
	p.advance()
	ident := ast.NewIdentifier(spanFromToken(tok), tok.Literal)
	return &ident
}

func (p *Parser) parseQualifiedRef() *ast.QualifiedRef {
	ident := p.parseIdentifier()
	if ident == nil {
		return nil
	}

	parts := []ast.Identifier{*ident}
	end := ident.Span()
	for p.cur.Type == token.Dot {
		p.advance()
		next := p.parseIdentifier()
		if next == nil {
			p.errorAtCurrent("expected identifier after .", []string{"identifier"})
			break
		}
		parts = append(parts, *next)
		end = next.Span()
	}

	ref := ast.NewQualifiedRef(ast.MergeSpan(ident.Span(), end), parts)
	return &ref
}

func (p *Parser) advance() {
	p.cur = p.peek
	p.peek = p.lexer.Next()
}

func (p *Parser) expect(tt token.Type, message string) (token.Token, bool) {
	if p.cur.Type != tt {
		p.errorAtCurrent(message, []string{tokenLabel(tt, "")})
		return token.Token{}, false
	}
	tok := p.cur
	p.advance()
	return tok, true
}

func (p *Parser) errorAtCurrent(message string, expected []string) {
	p.errors = append(p.errors, ParseError{
		Message:  message,
		Span:     spanFromToken(p.cur),
		Expected: expected,
		Found:    p.cur,
	})
}

func (p *Parser) syncTo(types ...token.Type) {
	for p.cur.Type != token.EOF {
		for _, tt := range types {
			if p.cur.Type == tt {
				return
			}
		}
		p.advance()
	}
}

func (p *Parser) isJoinStart() bool {
	switch p.cur.Type {
	case token.Join, token.Left, token.Right, token.Inner:
		return true
	default:
		return false
	}
}

func (p *Parser) expressionTerminated() bool {
	switch p.cur.Type {
	case token.EOF, token.Comma, token.From, token.Where, token.Join, token.Left, token.Right, token.Inner, token.On, token.Group, token.Order, token.By, token.Limit, token.Asc, token.Desc, token.RParen:
		return true
	default:
		return false
	}
}

func (p *Parser) currentPrecedence() int {
	switch p.cur.Type {
	case token.Or:
		return precOr
	case token.And:
		return precAnd
	case token.Eq, token.NEq, token.Lt, token.LtE, token.Gt, token.GtE, token.Is, token.In:
		return precCompare
	case token.Not:
		if p.peek.Type == token.In {
			return precCompare
		}
		return precLowest
	case token.Plus, token.Minus:
		return precAdd
	case token.Star, token.Slash, token.Percent:
		return precMultiply
	default:
		return precLowest
	}
}

func isBinaryOperator(tt token.Type) bool {
	switch tt {
	case token.Or, token.And, token.Eq, token.NEq, token.Lt, token.LtE, token.Gt, token.GtE, token.Plus, token.Minus, token.Star, token.Slash, token.Percent:
		return true
	default:
		return false
	}
}

func spanFromToken(tok token.Token) ast.Span {
	return ast.Span{
		Start: tok.Pos,
		End:   tok.Pos + tokenWidth(tok),
	}
}

func tokenWidth(tok token.Token) int {
	if tok.Type == token.EOF {
		return 0
	}
	if tok.Literal != "" {
		if tok.Type == token.String {
			return len(tok.Literal) + 2
		}
		return len(tok.Literal)
	}
	switch tok.Type {
	case token.Comma, token.Dot, token.LParen, token.RParen, token.Question, token.Eq, token.Lt, token.Gt, token.Plus, token.Minus, token.Star, token.Slash:
		return 1
	case token.NEq, token.LtE, token.GtE:
		return 2
	case token.NamedArg:
		return len(tok.Literal)
	default:
		return 0
	}
}

func tokenLabel(tt token.Type, literal string) string {
	if literal != "" {
		return literal
	}
	switch tt {
	case token.Identifier:
		return "identifier"
	case token.Number:
		return "number"
	case token.String:
		return "string"
	case token.Question:
		return "?"
	case token.NamedArg:
		return "named arg"
	case token.Select:
		return "SELECT"
	case token.From:
		return "FROM"
	case token.Where:
		return "WHERE"
	case token.Join:
		return "JOIN"
	case token.Left:
		return "LEFT"
	case token.Right:
		return "RIGHT"
	case token.Inner:
		return "INNER"
	case token.On:
		return "ON"
	case token.As:
		return "AS"
	case token.Order:
		return "ORDER"
	case token.Group:
		return "GROUP"
	case token.By:
		return "BY"
	case token.Limit:
		return "LIMIT"
	case token.Asc:
		return "ASC"
	case token.Desc:
		return "DESC"
	case token.And:
		return "AND"
	case token.Or:
		return "OR"
	case token.Not:
		return "NOT"
	case token.In:
		return "IN"
	case token.Is:
		return "IS"
	case token.Null:
		return "NULL"
	case token.True:
		return "TRUE"
	case token.False:
		return "FALSE"
	case token.Comma:
		return ","
	case token.Dot:
		return "."
	case token.LParen:
		return "("
	case token.RParen:
		return ")"
	case token.Eq:
		return "="
	case token.NEq:
		return "!="
	case token.Lt:
		return "<"
	case token.LtE:
		return "<="
	case token.Gt:
		return ">"
	case token.GtE:
		return ">="
	case token.Plus:
		return "+"
	case token.Minus:
		return "-"
	case token.Star:
		return "*"
	case token.Slash:
		return "/"
	case token.Percent:
		return "%"
	default:
		return "token"
	}
}
