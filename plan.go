package structql

import (
	"container/heap"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olimci/structql/ast"
	"github.com/olimci/structql/lexer/token"
)

type relationColumn struct {
	Source   string
	Name     string
	Type     reflect.Type
	Nullable bool
}

type relation struct {
	schema []relationColumn
	rows   []Row
}

type evalContext struct {
	row   Row
	outer []Row
}

type planNode interface {
	schema() []relationColumn
	execute(outer []Row) (*relation, error)
}

type tableRefPlan struct {
	node    planNode
	lateral bool
}

type scanNode struct {
	table *Table
	sch   []relationColumn
}

func (n *scanNode) schema() []relationColumn { return n.sch }

func (n *scanNode) execute(_ []Row) (*relation, error) {
	return &relation{schema: cloneSchema(n.sch), rows: n.table.materializedRows()}, nil
}

type renameSourceNode struct {
	input planNode
	sch   []relationColumn
}

func (n *renameSourceNode) schema() []relationColumn { return n.sch }

func (n *renameSourceNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}
	return &relation{schema: cloneSchema(n.sch), rows: rel.rows}, nil
}

type cartesianNode struct {
	left         planNode
	right        planNode
	rightLateral bool
	sch          []relationColumn
}

func (n *cartesianNode) schema() []relationColumn { return n.sch }

func (n *cartesianNode) execute(outer []Row) (*relation, error) {
	left, err := n.left.execute(outer)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0)
	for _, lrow := range left.rows {
		rightOuter := outer
		if n.rightLateral {
			rightOuter = prependOuter(lrow, outer)
		}
		right, err := n.right.execute(rightOuter)
		if err != nil {
			return nil, err
		}
		for _, rrow := range right.rows {
			rows = append(rows, joinRows(lrow, rrow))
		}
	}
	if !n.rightLateral {
		right, err := n.right.execute(outer)
		if err != nil {
			return nil, err
		}
		rows = rows[:0]
		rows = make([]Row, 0, len(left.rows)*len(right.rows))
		for _, lrow := range left.rows {
			for _, rrow := range right.rows {
				rows = append(rows, joinRows(lrow, rrow))
			}
		}
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

type joinNode struct {
	kind         ast.JoinKind
	left         planNode
	right        planNode
	rightLateral bool
	on           boundExpr
	sch          []relationColumn
}

func (n *joinNode) schema() []relationColumn { return n.sch }

func (n *joinNode) execute(outer []Row) (*relation, error) {
	left, err := n.left.execute(outer)
	if err != nil {
		return nil, err
	}
	if n.kind == ast.RightJoin && n.rightLateral {
		return nil, fmt.Errorf("RIGHT JOIN does not support lateral table functions")
	}
	if !n.rightLateral {
		right, err := n.right.execute(outer)
		if err != nil {
			return nil, err
		}
		switch n.kind {
		case ast.RightJoin:
			return n.executeRight(left, right, outer)
		default:
			return n.executeLeft(left, right, outer)
		}
	}
	return n.executeLeftLateral(left, outer)
}

func (n *joinNode) executeLeft(left, right *relation, outer []Row) (*relation, error) {
	rows := make([]Row, 0)
	rightNulls := make(Row, len(right.schema))
	for _, lrow := range left.rows {
		matched := false
		for _, rrow := range right.rows {
			joined := joinRows(lrow, rrow)
			ok, err := n.evalJoin(joined, outer)
			if err != nil {
				return nil, err
			}
			if ok {
				matched = true
				rows = append(rows, joined)
			}
		}
		if !matched && n.kind == ast.LeftJoin {
			rows = append(rows, joinRows(lrow, rightNulls))
		}
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

func (n *joinNode) executeRight(left, right *relation, outer []Row) (*relation, error) {
	rows := make([]Row, 0)
	leftNulls := make(Row, len(left.schema))
	for _, rrow := range right.rows {
		matched := false
		for _, lrow := range left.rows {
			joined := joinRows(lrow, rrow)
			ok, err := n.evalJoin(joined, outer)
			if err != nil {
				return nil, err
			}
			if ok {
				matched = true
				rows = append(rows, joined)
			}
		}
		if !matched {
			rows = append(rows, joinRows(leftNulls, rrow))
		}
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

func (n *joinNode) executeLeftLateral(left *relation, outer []Row) (*relation, error) {
	rows := make([]Row, 0)
	for _, lrow := range left.rows {
		right, err := n.right.execute(prependOuter(lrow, outer))
		if err != nil {
			return nil, err
		}
		rightNulls := make(Row, len(right.schema))
		matched := false
		for _, rrow := range right.rows {
			joined := joinRows(lrow, rrow)
			ok, err := n.evalJoin(joined, outer)
			if err != nil {
				return nil, err
			}
			if ok {
				matched = true
				rows = append(rows, joined)
			}
		}
		if !matched && n.kind == ast.LeftJoin {
			rows = append(rows, joinRows(lrow, rightNulls))
		}
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

func (n *joinNode) evalJoin(row Row, outer []Row) (bool, error) {
	if n.on == nil {
		return true, nil
	}
	value, err := n.on.Eval(evalContext{row: row, outer: outer})
	if err != nil {
		return false, err
	}
	return truthy(value), nil
}

type filterNode struct {
	input planNode
	pred  boundExpr
}

func (n *filterNode) schema() []relationColumn { return n.input.schema() }

func (n *filterNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(rel.rows))
	for _, row := range rel.rows {
		value, err := n.pred.Eval(evalContext{row: row, outer: outer})
		if err != nil {
			return nil, err
		}
		if truthy(value) {
			rows = append(rows, row)
		}
	}
	return &relation{schema: cloneSchema(rel.schema), rows: rows}, nil
}

type orderTermPlan struct {
	expr boundExpr
	desc bool
}

type sortNode struct {
	input planNode
	terms []orderTermPlan
	limit int
}

func (n *sortNode) schema() []relationColumn { return n.input.schema() }

func (n *sortNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}
	if len(rel.rows) == 0 || len(n.terms) == 0 {
		return rel, nil
	}

	keys := make([][]any, len(n.terms))
	for termIdx, term := range n.terms {
		keys[termIdx] = make([]any, len(rel.rows))
		for rowIdx, row := range rel.rows {
			value, err := term.expr.Eval(evalContext{row: row, outer: outer})
			if err != nil {
				return nil, err
			}
			keys[termIdx][rowIdx] = value
		}
	}

	indices := make([]int, len(rel.rows))
	for i := range indices {
		indices[i] = i
	}
	if n.limit > 0 && n.limit < len(indices) {
		indices = topNIndices(indices, n.limit, keys, n.terms)
	} else {
		sort.SliceStable(indices, func(i, j int) bool {
			return compareOrderKeys(keys, n.terms, indices[i], indices[j]) < 0
		})
	}

	rows := make([]Row, len(rel.rows))
	for i, idx := range indices {
		rows[i] = rel.rows[idx]
	}
	rows = rows[:len(indices)]
	return &relation{schema: cloneSchema(rel.schema), rows: rows}, nil
}

type limitNode struct {
	input planNode
	limit int
}

func (n *limitNode) schema() []relationColumn { return n.input.schema() }

func (n *limitNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}
	if n.limit < len(rel.rows) {
		rel.rows = rel.rows[:n.limit]
	}
	return rel, nil
}

type distinctNode struct {
	input planNode
}

func (n *distinctNode) schema() []relationColumn { return n.input.schema() }

func (n *distinctNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(rel.rows))
	seen := make(map[string]struct{}, len(rel.rows))
	for _, row := range rel.rows {
		if key, ok := rowHashKey(row); ok {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			rows = append(rows, row)
			continue
		}
		duplicate := false
		for _, existing := range rows {
			if rowsEqual(existing, row) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			rows = append(rows, row)
		}
	}
	return &relation{schema: cloneSchema(rel.schema), rows: rows}, nil
}

type projectItem struct {
	name string
	expr boundExpr
}

type projectNode struct {
	input planNode
	sch   []relationColumn
	items []projectItem
}

func (n *projectNode) schema() []relationColumn { return n.sch }

func (n *projectNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, len(rel.rows))
	for i, in := range rel.rows {
		out := make(Row, len(n.items))
		for j, item := range n.items {
			value, err := item.expr.Eval(evalContext{row: in, outer: outer})
			if err != nil {
				return nil, err
			}
			out[j] = value
		}
		rows[i] = out
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

type aggregateSpec struct {
	key      string
	name     string
	arg      boundExpr
	star     bool
	distinct bool
	typ      reflect.Type
	nullable bool
}

type aggregateState interface {
	Step(evalContext) error
	Result() any
}

type aggregateEvalExpr interface {
	Eval(aggEvalContext) (any, error)
	Type() reflect.Type
	Nullable() bool
}

type aggProjectItem struct {
	name string
	expr aggregateEvalExpr
}

type aggOrderTerm struct {
	expr aggregateEvalExpr
	desc bool
}

type aggregateNode struct {
	input      planNode
	groupExprs []boundExpr
	aggSpecs   []aggregateSpec
	having     aggregateEvalExpr
	selects    []aggProjectItem
	orderBy    []aggOrderTerm
	sch        []relationColumn
}

func (n *aggregateNode) schema() []relationColumn { return n.sch }

func (n *aggregateNode) execute(outer []Row) (*relation, error) {
	rel, err := n.input.execute(outer)
	if err != nil {
		return nil, err
	}

	type groupState struct {
		rep  Row
		aggs []aggregateState
	}

	groups := make([]*groupState, 0)
	groupByKey := make(map[string]*groupState)

	ensureGroup := func(key string, rep Row) *groupState {
		if state, ok := groupByKey[key]; ok {
			return state
		}
		aggs := make([]aggregateState, len(n.aggSpecs))
		for i, spec := range n.aggSpecs {
			aggs[i] = newAggregateState(spec)
		}
		state := &groupState{rep: rep, aggs: aggs}
		groupByKey[key] = state
		groups = append(groups, state)
		return state
	}

	if len(rel.rows) == 0 && len(n.groupExprs) == 0 {
		ensureGroup("", nil)
	}

	for _, row := range rel.rows {
		key, err := evaluateGroupKey(n.groupExprs, evalContext{row: row, outer: outer})
		if err != nil {
			return nil, err
		}
		state := ensureGroup(key, row)
		if state.rep == nil {
			state.rep = row
		}
		ctx := evalContext{row: row, outer: outer}
		for _, agg := range state.aggs {
			if err := agg.Step(ctx); err != nil {
				return nil, err
			}
		}
	}

	type projectedRow struct {
		row   Row
		order []any
	}
	projected := make([]projectedRow, 0, len(groups))
	for _, group := range groups {
		ctx := aggEvalContext{
			row:    group.rep,
			outer:  outer,
			values: make(map[string]any, len(n.aggSpecs)),
		}
		for i, spec := range n.aggSpecs {
			ctx.values[spec.key] = group.aggs[i].Result()
		}
		if n.having != nil {
			value, err := n.having.Eval(ctx)
			if err != nil {
				return nil, err
			}
			if !truthy(value) {
				continue
			}
		}

		row := make(Row, len(n.selects))
		for i, item := range n.selects {
			value, err := item.expr.Eval(ctx)
			if err != nil {
				return nil, err
			}
			row[i] = value
		}
		orderVals := make([]any, len(n.orderBy))
		for i, term := range n.orderBy {
			value, err := term.expr.Eval(ctx)
			if err != nil {
				return nil, err
			}
			orderVals[i] = value
		}
		projected = append(projected, projectedRow{row: row, order: orderVals})
	}

	if len(n.orderBy) > 0 {
		sort.SliceStable(projected, func(i, j int) bool {
			for idx, term := range n.orderBy {
				cmp, ok := compareValues(projected[i].order[idx], projected[j].order[idx])
				if !ok || cmp == 0 {
					continue
				}
				if term.desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}

	rows := make([]Row, len(projected))
	for i, item := range projected {
		rows[i] = item.row
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

type plannerContext struct {
	db         *DB
	args       *queryArgs
	outer      [][]relationColumn
	captured   map[string]outerRef
	captureSeq []outerRef
}

type outerRef struct {
	level int
	index int
	col   relationColumn
}

func newPlannerContext(db *DB, outer [][]relationColumn, args *queryArgs) *plannerContext {
	return &plannerContext{
		db:       db,
		args:     args,
		outer:    outer,
		captured: make(map[string]outerRef),
	}
}

func (p *plannerContext) recordOuterRef(ref outerRef) {
	key := fmt.Sprintf("%d:%d", ref.level, ref.index)
	if _, exists := p.captured[key]; exists {
		return
	}
	p.captured[key] = ref
	p.captureSeq = append(p.captureSeq, ref)
}

func planQuery(db *DB, query *ast.Query, args []any) (planNode, error) {
	parsedArgs, err := parseQueryArgs(args)
	if err != nil {
		return nil, err
	}
	return planQueryWithParsedArgs(db, query, parsedArgs)
}

func planQueryWithParsedArgs(db *DB, query *ast.Query, parsedArgs *queryArgs) (planNode, error) {
	ctx := newPlannerContext(db, nil, parsedArgs)
	plan, err := planQueryWithContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if err := parsedArgs.validateUsage(); err != nil {
		return nil, err
	}
	return plan, nil
}

func planQueryWithContext(ctx *plannerContext, query *ast.Query) (planNode, error) {
	input, err := planSourceQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	if query.Where != nil {
		if containsAggregate(query.Where) {
			return nil, fmt.Errorf("aggregate expressions are not allowed in WHERE")
		}
		pred, err := bindExpr(ctx, query.Where, input.schema())
		if err != nil {
			return nil, err
		}
		input = &filterNode{input: input, pred: pred}
	}

	aggregateQuery := len(query.GroupBy) > 0 || query.Having != nil || selectsContainAggregate(query.Select) || orderTermsContainAggregate(query.OrderBy)
	if aggregateQuery {
		core, err := planAggregateQuery(ctx, input, query)
		if err != nil {
			return nil, err
		}
		return finalizeQueryPlan(ctx, core, query, input.schema())
	}
	if query.Having != nil {
		return nil, fmt.Errorf("HAVING requires GROUP BY or aggregate expressions")
	}
	core, err := planNonAggregateQuery(ctx, input, query)
	if err != nil {
		return nil, err
	}
	return finalizeQueryPlan(ctx, core, query, input.schema())
}

func planSourceQuery(ctx *plannerContext, query *ast.Query) (planNode, error) {
	if len(query.From) == 0 {
		return nil, fmt.Errorf("query must include FROM")
	}

	first, err := planTableRef(ctx, query.From[0], nil)
	if err != nil {
		return nil, err
	}
	if first.lateral {
		return nil, fmt.Errorf("FROM table functions cannot reference earlier sources")
	}
	plan := first.node
	for _, ref := range query.From[1:] {
		right, err := planTableRef(ctx, ref, nil)
		if err != nil {
			return nil, err
		}
		if right.lateral {
			return nil, fmt.Errorf("comma-separated FROM sources do not support lateral table functions")
		}
		plan = &cartesianNode{
			left:  plan,
			right: right.node,
			sch:   append(cloneSchema(plan.schema()), cloneSchema(right.node.schema())...),
		}
	}
	for _, join := range query.Joins {
		if join.Kind == ast.RightJoin && join.Table.Function != nil {
			return nil, fmt.Errorf("RIGHT JOIN does not support table functions")
		}
		right, err := planTableRef(ctx, join.Table, plan.schema())
		if err != nil {
			return nil, err
		}
		var on boundExpr
		if join.On != nil {
			on, err = bindExpr(ctx, join.On, append(cloneSchema(plan.schema()), cloneSchema(right.node.schema())...))
			if err != nil {
				return nil, err
			}
		}
		plan = &joinNode{
			kind:         join.Kind,
			left:         plan,
			right:        right.node,
			rightLateral: right.lateral,
			on:           on,
			sch:          joinSchema(plan.schema(), right.node.schema(), join.Kind),
		}
	}
	return plan, nil
}

func finalizeQueryPlan(ctx *plannerContext, input planNode, query *ast.Query, sourceSchema []relationColumn) (planNode, error) {
	var err error
	if query.Distinct {
		input = &distinctNode{input: input}
	}
	if query.Limit != nil {
		var limit int
		limit, err = parseLimit(ctx, query.Limit, sourceSchema)
		if err != nil {
			return nil, err
		}
		if !query.Distinct {
			applyTopNLimit(input, limit)
		}
		input = &limitNode{input: input, limit: limit}
	}
	return input, nil
}

func applyTopNLimit(node planNode, limit int) {
	switch n := node.(type) {
	case *sortNode:
		n.limit = limit
	case *projectNode:
		if sort, ok := n.input.(*sortNode); ok {
			sort.limit = limit
		}
	}
}

func planNonAggregateQuery(ctx *plannerContext, input planNode, query *ast.Query) (planNode, error) {
	aliasExprs := selectAliases(query.Select)
	if len(query.OrderBy) > 0 {
		terms := make([]orderTermPlan, 0, len(query.OrderBy))
		for _, term := range query.OrderBy {
			expr, err := bindOrderExpr(ctx, term.Expr, input.schema(), aliasExprs)
			if err != nil {
				return nil, err
			}
			terms = append(terms, orderTermPlan{expr: expr, desc: term.Desc})
		}
		input = &sortNode{input: input, terms: terms}
	}

	items := make([]projectItem, 0, len(query.Select))
	schema := make([]relationColumn, 0, len(query.Select))
	for i, item := range query.Select {
		if item.Wildcard != nil {
			for idx, col := range input.schema() {
				items = append(items, projectItem{
					name: col.Name,
					expr: columnExpr{index: idx, col: col},
				})
				schema = append(schema, relationColumn{
					Source:   col.Source,
					Name:     col.Name,
					Type:     col.Type,
					Nullable: col.Nullable,
				})
			}
			continue
		}
		expr, err := bindExpr(ctx, item.Expr, input.schema())
		if err != nil {
			return nil, err
		}
		name := projectionName(item, i)
		items = append(items, projectItem{name: name, expr: expr})
		schema = append(schema, relationColumn{Name: name, Type: expr.Type(), Nullable: expr.Nullable()})
	}
	return &projectNode{input: input, sch: schema, items: items}, nil
}

func planAggregateQuery(ctx *plannerContext, input planNode, query *ast.Query) (planNode, error) {
	for _, item := range query.Select {
		if item.Wildcard != nil {
			return nil, fmt.Errorf("SELECT * is not allowed in aggregate queries")
		}
	}
	groupExprs := make([]boundExpr, 0, len(query.GroupBy))
	groupKeys := make(map[string]struct{}, len(query.GroupBy))
	for _, expr := range query.GroupBy {
		if containsAggregate(expr) {
			return nil, fmt.Errorf("aggregate expressions are not allowed in GROUP BY")
		}
		bound, err := bindExpr(ctx, expr, input.schema())
		if err != nil {
			return nil, err
		}
		groupExprs = append(groupExprs, bound)
		groupKeys[exprFingerprint(expr)] = struct{}{}
	}

	specsByKey := make(map[string]aggregateSpec)
	for _, item := range query.Select {
		if err := collectAggregateSpecs(ctx, specsByKey, item.Expr, input.schema()); err != nil {
			return nil, err
		}
	}
	aliasExprs := selectAliases(query.Select)
	for _, term := range query.OrderBy {
		expr := term.Expr
		if ident, ok := expr.(ast.Identifier); ok {
			if aliased, exists := aliasExprs[normalizeName(ident.Name)]; exists {
				expr = aliased
			}
		}
		if err := collectAggregateSpecs(ctx, specsByKey, expr, input.schema()); err != nil {
			return nil, err
		}
	}
	if query.Having != nil {
		if err := collectAggregateSpecs(ctx, specsByKey, query.Having, input.schema()); err != nil {
			return nil, err
		}
	}
	specs := aggregateSpecsSorted(specsByKey)

	selects := make([]aggProjectItem, 0, len(query.Select))
	schema := make([]relationColumn, 0, len(query.Select))
	for i, item := range query.Select {
		expr, err := bindAggregateExpr(ctx, item.Expr, input.schema(), groupKeys, specsByKey)
		if err != nil {
			return nil, err
		}
		name := projectionName(item, i)
		selects = append(selects, aggProjectItem{name: name, expr: expr})
		schema = append(schema, relationColumn{Name: name, Type: expr.Type(), Nullable: expr.Nullable()})
	}

	orderTerms := make([]aggOrderTerm, 0, len(query.OrderBy))
	for _, term := range query.OrderBy {
		exprAst := term.Expr
		if ident, ok := exprAst.(ast.Identifier); ok {
			if aliased, exists := aliasExprs[normalizeName(ident.Name)]; exists {
				exprAst = aliased
			}
		}
		expr, err := bindAggregateExpr(ctx, exprAst, input.schema(), groupKeys, specsByKey)
		if err != nil {
			return nil, err
		}
		orderTerms = append(orderTerms, aggOrderTerm{expr: expr, desc: term.Desc})
	}

	var having aggregateEvalExpr
	if query.Having != nil {
		var err error
		having, err = bindAggregateExpr(ctx, query.Having, input.schema(), groupKeys, specsByKey)
		if err != nil {
			return nil, err
		}
	}

	return &aggregateNode{
		input:      input,
		groupExprs: groupExprs,
		aggSpecs:   specs,
		having:     having,
		selects:    selects,
		orderBy:    orderTerms,
		sch:        schema,
	}, nil
}

func planTableRef(ctx *plannerContext, ref ast.TableRef, lateralOuter []relationColumn) (tableRefPlan, error) {
	if ref.Subquery != nil {
		childCtx := newPlannerContext(ctx.db, nil, ctx.args)
		child, err := planQueryWithContext(childCtx, ref.Subquery)
		if err != nil {
			return tableRefPlan{}, err
		}
		if len(childCtx.captureSeq) > 0 {
			return tableRefPlan{}, fmt.Errorf("correlated derived tables are not supported yet")
		}
		if ref.Alias == nil {
			return tableRefPlan{}, fmt.Errorf("derived tables require an alias")
		}
		schema := cloneSchema(child.schema())
		for i := range schema {
			schema[i].Source = ref.Alias.Name
		}
		return tableRefPlan{node: &renameSourceNode{input: child, sch: schema}}, nil
	}
	if ref.Function != nil {
		return planFunctionTableRef(ctx, ref, lateralOuter)
	}
	if ref.Name == nil || len(ref.Name.Parts) != 1 {
		return tableRefPlan{}, fmt.Errorf("table references currently support a single identifier")
	}
	tableName := ref.Name.Parts[0].Name
	table, ok := ctx.db.tables[normalizeName(tableName)]
	if !ok {
		return tableRefPlan{}, fmt.Errorf("unknown table %q", tableName)
	}
	source := tableName
	if ref.Alias != nil {
		source = ref.Alias.Name
	}
	schema := make([]relationColumn, len(table.schema))
	for i, col := range table.schema {
		schema[i] = relationColumn{Source: source, Name: col.Name, Type: col.Type, Nullable: col.Nullable}
	}
	return tableRefPlan{node: &scanNode{table: table, sch: schema}}, nil
}

func planFunctionTableRef(ctx *plannerContext, ref ast.TableRef, lateralOuter []relationColumn) (tableRefPlan, error) {
	if ref.Function == nil {
		return tableRefPlan{}, fmt.Errorf("table function missing")
	}
	if ref.Alias == nil {
		return tableRefPlan{}, fmt.Errorf("table functions require an alias")
	}

	call := ref.Function
	if normalizeName(call.Name.Name) != "unnest" {
		return tableRefPlan{}, fmt.Errorf("unknown table function %q", call.Name.Name)
	}
	if len(call.Args) != 1 {
		return tableRefPlan{}, fmt.Errorf("unnest requires exactly one argument")
	}

	bindCtx := ctx
	if len(lateralOuter) > 0 {
		bindCtx = newPlannerContext(ctx.db, append([][]relationColumn{lateralOuter}, ctx.outer...), ctx.args)
	}

	arg, err := bindExpr(bindCtx, call.Args[0], nil)
	if err != nil {
		return tableRefPlan{}, err
	}

	colType, colNullable, err := unnestColumnType(arg.Type(), arg.Nullable())
	if err != nil {
		return tableRefPlan{}, err
	}
	node := &unnestNode{
		expr: arg,
		sch: []relationColumn{{
			Source:   ref.Alias.Name,
			Name:     "value",
			Type:     colType,
			Nullable: colNullable,
		}},
	}
	return tableRefPlan{
		node:    node,
		lateral: len(bindCtx.captureSeq) > 0,
	}, nil
}

func unnestColumnType(typ reflect.Type, nullable bool) (reflect.Type, bool, error) {
	if typ == nil {
		return nil, true, nil
	}
	for typ.Kind() == reflect.Pointer {
		nullable = true
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Array {
		return nil, false, fmt.Errorf("unnest requires a slice or array argument")
	}
	elem := typ.Elem()
	elemNullable := nullable
	for elem.Kind() == reflect.Pointer {
		elemNullable = true
		elem = elem.Elem()
	}
	return elem, elemNullable, nil
}

func selectAliases(items []ast.SelectItem) map[string]ast.Expr {
	out := make(map[string]ast.Expr, len(items))
	for _, item := range items {
		if item.Alias != nil && item.Expr != nil {
			out[normalizeName(item.Alias.Name)] = item.Expr
		}
	}
	return out
}

func bindOrderExpr(ctx *plannerContext, expr ast.Expr, schema []relationColumn, aliases map[string]ast.Expr) (boundExpr, error) {
	if ident, ok := expr.(ast.Identifier); ok {
		if aliased, exists := aliases[normalizeName(ident.Name)]; exists {
			return bindExpr(ctx, aliased, schema)
		}
	}
	return bindExpr(ctx, expr, schema)
}

func parseLimit(ctx *plannerContext, expr ast.Expr, schema []relationColumn) (int, error) {
	switch expr := expr.(type) {
	case ast.NumberLiteral:
		limit, err := strconv.Atoi(expr.Raw)
		if err != nil {
			return 0, fmt.Errorf("invalid LIMIT %q", expr.Raw)
		}
		if limit < 0 {
			return 0, fmt.Errorf("LIMIT cannot be negative")
		}
		return limit, nil
	case ast.PlaceholderExpr, ast.NamedPlaceholderExpr:
		bound, err := bindExpr(ctx, expr, schema)
		if err != nil {
			return 0, err
		}
		value, err := bound.Eval(evalContext{})
		if err != nil {
			return 0, err
		}
		limit, ok := asInt64(value)
		if !ok {
			if value == nil {
				return 0, fmt.Errorf("LIMIT placeholder cannot be nil")
			}
			return 0, fmt.Errorf("LIMIT placeholder must be an integer, got %T", value)
		}
		if limit < 0 {
			return 0, fmt.Errorf("LIMIT cannot be negative")
		}
		return int(limit), nil
	default:
		return 0, fmt.Errorf("LIMIT currently requires a numeric literal or placeholder")
	}
}

func projectionName(item ast.SelectItem, idx int) string {
	if item.Alias != nil {
		return item.Alias.Name
	}
	switch expr := item.Expr.(type) {
	case ast.Identifier:
		return expr.Name
	case ast.QualifiedRef:
		if len(expr.Parts) == 0 {
			return fmt.Sprintf("expr%d", idx+1)
		}
		return expr.Parts[len(expr.Parts)-1].Name
	case ast.CallExpr:
		return strings.ToLower(expr.Name.Name)
	default:
		return fmt.Sprintf("expr%d", idx+1)
	}
}

func joinSchema(left, right []relationColumn, kind ast.JoinKind) []relationColumn {
	schema := append(cloneSchema(left), cloneSchema(right)...)
	switch kind {
	case ast.LeftJoin:
		for i := len(left); i < len(schema); i++ {
			schema[i].Nullable = true
		}
	case ast.RightJoin:
		for i := 0; i < len(left); i++ {
			schema[i].Nullable = true
		}
	}
	return schema
}

func cloneSchema(schema []relationColumn) []relationColumn {
	out := make([]relationColumn, len(schema))
	copy(out, schema)
	return out
}

func joinRows(left, right Row) Row {
	row := make(Row, len(left)+len(right))
	copy(row, left)
	copy(row[len(left):], right)
	return row
}

func rowsEqual(left, right Row) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !reflect.DeepEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

func prependOuter(row Row, outer []Row) []Row {
	child := make([]Row, 0, len(outer)+1)
	child = append(child, row)
	child = append(child, outer...)
	return child
}

type unnestNode struct {
	expr boundExpr
	sch  []relationColumn
}

func (n *unnestNode) schema() []relationColumn { return n.sch }

func (n *unnestNode) execute(outer []Row) (*relation, error) {
	value, err := n.expr.Eval(evalContext{outer: outer})
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &relation{schema: cloneSchema(n.sch)}, nil
	}

	v := reflect.ValueOf(value)
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return &relation{schema: cloneSchema(n.sch)}, nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return &relation{schema: cloneSchema(n.sch)}, nil
	}
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("unnest requires a slice or array, got %T", value)
	}

	rows := make([]Row, 0, v.Len())
	for i := range v.Len() {
		rows = append(rows, Row{v.Index(i).Interface()})
	}
	return &relation{schema: cloneSchema(n.sch), rows: rows}, nil
}

type boundExpr interface {
	Eval(evalContext) (any, error)
	Type() reflect.Type
	Nullable() bool
}

type literalExpr struct {
	value    any
	typ      reflect.Type
	nullable bool
}

func (e literalExpr) Eval(evalContext) (any, error) { return e.value, nil }
func (e literalExpr) Type() reflect.Type            { return e.typ }
func (e literalExpr) Nullable() bool                { return e.nullable }

type argExpr struct {
	value any
	typ   reflect.Type
	null  bool
}

func (e argExpr) Eval(evalContext) (any, error) { return e.value, nil }
func (e argExpr) Type() reflect.Type            { return e.typ }
func (e argExpr) Nullable() bool                { return e.null }

type columnExpr struct {
	index int
	col   relationColumn
}

func (e columnExpr) Eval(ctx evalContext) (any, error) {
	if e.index < 0 || e.index >= len(ctx.row) {
		return nil, fmt.Errorf("column index out of range")
	}
	return ctx.row[e.index], nil
}
func (e columnExpr) Type() reflect.Type { return e.col.Type }
func (e columnExpr) Nullable() bool     { return e.col.Nullable }

type outerColumnExpr struct {
	ref outerRef
}

func (e outerColumnExpr) Eval(ctx evalContext) (any, error) {
	if e.ref.level < 0 || e.ref.level >= len(ctx.outer) {
		return nil, fmt.Errorf("outer column scope out of range")
	}
	row := ctx.outer[e.ref.level]
	if e.ref.index < 0 || e.ref.index >= len(row) {
		return nil, fmt.Errorf("outer column index out of range")
	}
	return row[e.ref.index], nil
}
func (e outerColumnExpr) Type() reflect.Type { return e.ref.col.Type }
func (e outerColumnExpr) Nullable() bool     { return e.ref.col.Nullable }

type pathExpr struct {
	base     boundExpr
	path     []string
	typ      reflect.Type
	nullable bool
}

func (e pathExpr) Eval(ctx evalContext) (any, error) {
	value, err := e.base.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return traverseValuePath(value, e.path)
}
func (e pathExpr) Type() reflect.Type { return e.typ }
func (e pathExpr) Nullable() bool     { return e.nullable }

type unaryBoundExpr struct {
	op    token.Type
	expr  boundExpr
	typ   reflect.Type
	isNil bool
}

func (e unaryBoundExpr) Eval(ctx evalContext) (any, error) {
	value, err := e.expr.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalUnaryOp(e.op, value)
}
func (e unaryBoundExpr) Type() reflect.Type { return e.typ }
func (e unaryBoundExpr) Nullable() bool     { return e.isNil }

type binaryBoundExpr struct {
	left     boundExpr
	op       token.Type
	right    boundExpr
	typ      reflect.Type
	nullable bool
}

func (e binaryBoundExpr) Eval(ctx evalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := e.right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalBinaryOp(e.op, left, right)
}
func (e binaryBoundExpr) Type() reflect.Type { return e.typ }
func (e binaryBoundExpr) Nullable() bool     { return e.nullable }

type isBoundExpr struct {
	left    boundExpr
	right   boundExpr
	negated bool
}

func (e isBoundExpr) Eval(ctx evalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := e.right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalIsOp(left, right, e.negated), nil
}
func (e isBoundExpr) Type() reflect.Type { return reflect.TypeFor[bool]() }
func (e isBoundExpr) Nullable() bool     { return false }

type inBoundExpr struct {
	left    boundExpr
	right   []boundExpr
	negated bool
}

func (e inBoundExpr) Eval(ctx evalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right := make([]any, 0, len(e.right))
	for _, expr := range e.right {
		value, err := expr.Eval(ctx)
		if err != nil {
			return nil, err
		}
		right = append(right, value)
	}
	return evalInOp(left, right, e.negated), nil
}
func (e inBoundExpr) Type() reflect.Type { return reflect.TypeFor[bool]() }
func (e inBoundExpr) Nullable() bool     { return false }

type scalarFunctionExpr struct {
	name string
	fn   ScalarFunction
	args []boundExpr
}

func (e scalarFunctionExpr) Eval(ctx evalContext) (any, error) {
	values := make([]any, len(e.args))
	for i, arg := range e.args {
		value, err := arg.Eval(ctx)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return e.fn.Eval(values)
}
func (e scalarFunctionExpr) Type() reflect.Type { return e.fn.ResultType }
func (e scalarFunctionExpr) Nullable() bool     { return e.fn.Nullable }

type scalarSubqueryExpr struct {
	plan      planNode
	typ       reflect.Type
	outerRefs []outerRef
	cache     map[string]scalarSubqueryCache
	uncorrel  bool
}

type scalarSubqueryCache struct {
	value any
	err   error
}

func (e scalarSubqueryExpr) Eval(ctx evalContext) (any, error) {
	cacheKey, childOuter, err := e.cacheKey(ctx)
	if err != nil {
		return nil, err
	}
	if cached, ok := e.cache[cacheKey]; ok {
		return cached.value, cached.err
	}

	runOuter := childOuter
	if e.uncorrel {
		runOuter = nil
	}
	rel, err := e.plan.execute(runOuter)
	if err == nil {
		switch {
		case len(rel.schema) != 1:
			err = fmt.Errorf("scalar subquery must return exactly one column")
		case len(rel.rows) == 0:
			e.cache[cacheKey] = scalarSubqueryCache{value: nil, err: nil}
			return nil, nil
		case len(rel.rows) > 1:
			err = fmt.Errorf("scalar subquery must return at most one row")
		default:
			value := rel.rows[0][0]
			e.cache[cacheKey] = scalarSubqueryCache{value: value, err: nil}
			return value, nil
		}
	}
	e.cache[cacheKey] = scalarSubqueryCache{err: err}
	return nil, err
}

func (e scalarSubqueryExpr) cacheKey(ctx evalContext) (string, []Row, error) {
	childOuter := make([]Row, 0, len(ctx.outer)+1)
	if ctx.row != nil {
		childOuter = append(childOuter, ctx.row)
	}
	childOuter = append(childOuter, ctx.outer...)

	if e.uncorrel {
		return "__uncorrelated__", childOuter, nil
	}

	var b strings.Builder
	for i, ref := range e.outerRefs {
		if ref.level < 0 || ref.level >= len(childOuter) {
			return "", nil, fmt.Errorf("outer scope unavailable for correlated subquery")
		}
		row := childOuter[ref.level]
		if ref.index < 0 || ref.index >= len(row) {
			return "", nil, fmt.Errorf("outer column unavailable for correlated subquery")
		}
		if i > 0 {
			b.WriteString("|")
		}
		fmt.Fprintf(&b, "%d:%d:", ref.level, ref.index)
		if !appendValueKey(&b, row[ref.index]) {
			return "", nil, fmt.Errorf("unsupported correlated subquery key type %T", row[ref.index])
		}
	}
	return b.String(), childOuter, nil
}

func (e scalarSubqueryExpr) Type() reflect.Type { return e.typ }
func (e scalarSubqueryExpr) Nullable() bool     { return true }

func bindExpr(ctx *plannerContext, expr ast.Expr, schema []relationColumn) (boundExpr, error) {
	switch expr := expr.(type) {
	case ast.Identifier:
		if idx, col, ok, err := resolveIdentifierLocal(schema, expr.Name); err != nil {
			return nil, err
		} else if ok {
			return columnExpr{index: idx, col: col}, nil
		}
		if ref, ok, err := resolveIdentifierOuter(ctx, expr.Name); err != nil {
			return nil, err
		} else if ok {
			ctx.recordOuterRef(ref)
			return outerColumnExpr{ref: ref}, nil
		}
		return nil, fmt.Errorf("unknown column %q", expr.Name)
	case ast.QualifiedRef:
		return bindQualifiedRef(ctx, expr, schema)
	case ast.NumberLiteral:
		value, err := strconv.Atoi(expr.Raw)
		if err != nil {
			return nil, fmt.Errorf("invalid number literal %q", expr.Raw)
		}
		return literalExpr{value: value, typ: reflect.TypeFor[int](), nullable: false}, nil
	case ast.StringLiteral:
		return literalExpr{value: expr.Value, typ: reflect.TypeFor[string](), nullable: false}, nil
	case ast.BoolLiteral:
		return literalExpr{value: expr.Value, typ: reflect.TypeFor[bool](), nullable: false}, nil
	case ast.NullLiteral:
		return literalExpr{value: nil, typ: nil, nullable: true}, nil
	case ast.PlaceholderExpr:
		if expr.Index < 0 || expr.Index >= len(ctx.args.positional) {
			return nil, fmt.Errorf("missing query arg for placeholder %d", expr.Index+1)
		}
		value := ctx.args.positional[expr.Index]
		ctx.args.usedPos[expr.Index] = struct{}{}
		var typ reflect.Type
		if value != nil {
			typ = reflect.TypeOf(value)
		}
		return argExpr{value: value, typ: typ, null: value == nil}, nil
	case ast.NamedPlaceholderExpr:
		key := normalizeName(expr.Name)
		value, ok := ctx.args.named[key]
		if !ok {
			return nil, fmt.Errorf("missing named query arg %q", expr.Name)
		}
		ctx.args.usedNamed[key] = struct{}{}
		var typ reflect.Type
		if value != nil {
			typ = reflect.TypeOf(value)
		}
		return argExpr{value: value, typ: typ, null: value == nil}, nil
	case ast.UnaryExpr:
		inner, err := bindExpr(ctx, expr.Expr, schema)
		if err != nil {
			return nil, err
		}
		typ := inner.Type()
		if expr.Op == token.Not {
			typ = reflect.TypeFor[bool]()
		}
		return unaryBoundExpr{op: expr.Op, expr: inner, typ: typ, isNil: inner.Nullable()}, nil
	case ast.BinaryExpr:
		left, err := bindExpr(ctx, expr.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := bindExpr(ctx, expr.Right, schema)
		if err != nil {
			return nil, err
		}
		typ := left.Type()
		nullable := left.Nullable() || right.Nullable()
		switch expr.Op {
		case token.And, token.Or, token.Eq, token.NEq, token.Lt, token.LtE, token.Gt, token.GtE:
			typ = reflect.TypeFor[bool]()
			nullable = false
		case token.Plus, token.Minus, token.Star, token.Slash:
			typ = resultNumberType(left.Type(), right.Type())
		}
		return binaryBoundExpr{left: left, op: expr.Op, right: right, typ: typ, nullable: nullable}, nil
	case ast.IsExpr:
		left, err := bindExpr(ctx, expr.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := bindExpr(ctx, expr.Right, schema)
		if err != nil {
			return nil, err
		}
		return isBoundExpr{left: left, right: right, negated: expr.Negated}, nil
	case ast.InExpr:
		left, err := bindExpr(ctx, expr.Left, schema)
		if err != nil {
			return nil, err
		}
		right := make([]boundExpr, 0, len(expr.Right))
		for _, candidate := range expr.Right {
			bound, err := bindExpr(ctx, candidate, schema)
			if err != nil {
				return nil, err
			}
			right = append(right, bound)
		}
		return inBoundExpr{left: left, right: right, negated: expr.Negated}, nil
	case ast.CallExpr:
		if expr.Star {
			return nil, fmt.Errorf("function %q does not accept *", expr.Name.Name)
		}
		if expr.Distinct {
			return nil, fmt.Errorf("function %q does not accept DISTINCT arguments", expr.Name.Name)
		}
		if isAggregateName(expr.Name.Name) {
			return nil, fmt.Errorf("aggregate function %q is not allowed in this context", expr.Name.Name)
		}
		fn, err := lookupScalarFunction(ctx.db, expr.Name.Name, len(expr.Args))
		if err != nil {
			return nil, err
		}
		args := make([]boundExpr, 0, len(expr.Args))
		for _, arg := range expr.Args {
			bound, err := bindExpr(ctx, arg, schema)
			if err != nil {
				return nil, err
			}
			args = append(args, bound)
		}
		return scalarFunctionExpr{name: expr.Name.Name, fn: fn, args: args}, nil
	case ast.SubqueryExpr:
		childCtx := newPlannerContext(ctx.db, append([][]relationColumn{schema}, ctx.outer...), ctx.args)
		plan, err := planQueryWithContext(childCtx, expr.Query)
		if err != nil {
			return nil, err
		}
		childSchema := plan.schema()
		var typ reflect.Type
		if len(childSchema) == 1 {
			typ = childSchema[0].Type
		}
		return scalarSubqueryExpr{
			plan:      plan,
			typ:       typ,
			outerRefs: childCtx.captureSeq,
			cache:     make(map[string]scalarSubqueryCache),
			uncorrel:  len(childCtx.captureSeq) == 0,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}

func bindQualifiedRef(ctx *plannerContext, expr ast.QualifiedRef, schema []relationColumn) (boundExpr, error) {
	if len(expr.Parts) == 1 {
		if idx, col, ok, err := resolveIdentifierLocal(schema, expr.Parts[0].Name); err != nil {
			return nil, err
		} else if ok {
			return columnExpr{index: idx, col: col}, nil
		}
		if ref, ok, err := resolveIdentifierOuter(ctx, expr.Parts[0].Name); err != nil {
			return nil, err
		} else if ok {
			ctx.recordOuterRef(ref)
			return outerColumnExpr{ref: ref}, nil
		}
		return nil, fmt.Errorf("unknown column %q", expr.Parts[0].Name)
	}

	source := expr.Parts[0].Name
	name := expr.Parts[1].Name
	if idx, col, ok, err := resolveQualifiedLocal(schema, source, name); err != nil {
		return nil, err
	} else if ok {
		base := boundExpr(columnExpr{index: idx, col: col})
		return bindPathFromBase(base, expr.Parts[2:]), nil
	}
	if ref, ok, err := resolveQualifiedOuter(ctx, source, name); err != nil {
		return nil, err
	} else if ok {
		ctx.recordOuterRef(ref)
		base := boundExpr(outerColumnExpr{ref: ref})
		return bindPathFromBase(base, expr.Parts[2:]), nil
	}
	if idx, col, ok, err := resolveIdentifierLocal(schema, expr.Parts[0].Name); err != nil {
		return nil, err
	} else if ok {
		base := boundExpr(columnExpr{index: idx, col: col})
		return bindPathFromBase(base, expr.Parts[1:]), nil
	}
	if ref, ok, err := resolveIdentifierOuter(ctx, expr.Parts[0].Name); err != nil {
		return nil, err
	} else if ok {
		ctx.recordOuterRef(ref)
		base := boundExpr(outerColumnExpr{ref: ref})
		return bindPathFromBase(base, expr.Parts[1:]), nil
	}
	return nil, fmt.Errorf("unknown column %s", qualifiedRefName(expr.Parts))
}

func bindPathFromBase(base boundExpr, parts []ast.Identifier) boundExpr {
	if len(parts) == 0 {
		return base
	}
	path := make([]string, len(parts))
	for i, part := range parts {
		path[i] = part.Name
	}
	typ, nullable := inferPathType(base.Type(), path)
	return pathExpr{base: base, path: path, typ: typ, nullable: base.Nullable() || nullable}
}

func resolveIdentifierLocal(schema []relationColumn, name string) (int, relationColumn, bool, error) {
	match := -1
	var col relationColumn
	for i, candidate := range schema {
		if strings.EqualFold(candidate.Name, name) {
			if match != -1 {
				return 0, relationColumn{}, false, fmt.Errorf("ambiguous column %q", name)
			}
			match = i
			col = candidate
		}
	}
	if match == -1 {
		return 0, relationColumn{}, false, nil
	}
	return match, col, true, nil
}

func resolveQualifiedLocal(schema []relationColumn, source, name string) (int, relationColumn, bool, error) {
	match := -1
	var col relationColumn
	for i, candidate := range schema {
		if strings.EqualFold(candidate.Source, source) && strings.EqualFold(candidate.Name, name) {
			if match != -1 {
				return 0, relationColumn{}, false, fmt.Errorf("ambiguous qualified column reference %s.%s", source, name)
			}
			match = i
			col = candidate
		}
	}
	if match == -1 {
		return 0, relationColumn{}, false, nil
	}
	return match, col, true, nil
}

func resolveIdentifierOuter(ctx *plannerContext, name string) (outerRef, bool, error) {
	for level, scope := range ctx.outer {
		idx, col, ok, err := resolveIdentifierLocal(scope, name)
		if err != nil {
			return outerRef{}, false, err
		}
		if ok {
			return outerRef{level: level, index: idx, col: col}, true, nil
		}
	}
	return outerRef{}, false, nil
}

func resolveQualifiedOuter(ctx *plannerContext, source, name string) (outerRef, bool, error) {
	for level, scope := range ctx.outer {
		idx, col, ok, err := resolveQualifiedLocal(scope, source, name)
		if err != nil {
			return outerRef{}, false, err
		}
		if ok {
			return outerRef{level: level, index: idx, col: col}, true, nil
		}
	}
	return outerRef{}, false, nil
}

type aggEvalContext struct {
	row    Row
	outer  []Row
	values map[string]any
}

type aggLiteralExpr struct {
	value any
	typ   reflect.Type
	null  bool
}

func (e aggLiteralExpr) Eval(aggEvalContext) (any, error) { return e.value, nil }
func (e aggLiteralExpr) Type() reflect.Type               { return e.typ }
func (e aggLiteralExpr) Nullable() bool                   { return e.null }

type aggRowExpr struct{ expr boundExpr }

func (e aggRowExpr) Eval(ctx aggEvalContext) (any, error) {
	return e.expr.Eval(evalContext{row: ctx.row, outer: ctx.outer})
}
func (e aggRowExpr) Type() reflect.Type { return e.expr.Type() }
func (e aggRowExpr) Nullable() bool     { return e.expr.Nullable() }

type aggResultExpr struct {
	key  string
	typ  reflect.Type
	null bool
}

func (e aggResultExpr) Eval(ctx aggEvalContext) (any, error) { return ctx.values[e.key], nil }
func (e aggResultExpr) Type() reflect.Type                   { return e.typ }
func (e aggResultExpr) Nullable() bool                       { return e.null }

type aggUnaryExpr struct {
	op   token.Type
	expr aggregateEvalExpr
	typ  reflect.Type
	null bool
}

func (e aggUnaryExpr) Eval(ctx aggEvalContext) (any, error) {
	value, err := e.expr.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalUnaryOp(e.op, value)
}
func (e aggUnaryExpr) Type() reflect.Type { return e.typ }
func (e aggUnaryExpr) Nullable() bool     { return e.null }

type aggBinaryExpr struct {
	left  aggregateEvalExpr
	op    token.Type
	right aggregateEvalExpr
	typ   reflect.Type
	null  bool
}

func (e aggBinaryExpr) Eval(ctx aggEvalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := e.right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalBinaryOp(e.op, left, right)
}
func (e aggBinaryExpr) Type() reflect.Type { return e.typ }
func (e aggBinaryExpr) Nullable() bool     { return e.null }

type aggIsExpr struct {
	left    aggregateEvalExpr
	right   aggregateEvalExpr
	negated bool
}

func (e aggIsExpr) Eval(ctx aggEvalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := e.right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return evalIsOp(left, right, e.negated), nil
}
func (e aggIsExpr) Type() reflect.Type { return reflect.TypeFor[bool]() }
func (e aggIsExpr) Nullable() bool     { return false }

type aggInExpr struct {
	left    aggregateEvalExpr
	right   []aggregateEvalExpr
	negated bool
}

func (e aggInExpr) Eval(ctx aggEvalContext) (any, error) {
	left, err := e.left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	right := make([]any, 0, len(e.right))
	for _, expr := range e.right {
		value, err := expr.Eval(ctx)
		if err != nil {
			return nil, err
		}
		right = append(right, value)
	}
	return evalInOp(left, right, e.negated), nil
}
func (e aggInExpr) Type() reflect.Type { return reflect.TypeFor[bool]() }
func (e aggInExpr) Nullable() bool     { return false }

type aggScalarFunctionExpr struct {
	fn   ScalarFunction
	args []aggregateEvalExpr
}

func (e aggScalarFunctionExpr) Eval(ctx aggEvalContext) (any, error) {
	values := make([]any, len(e.args))
	for i, arg := range e.args {
		value, err := arg.Eval(ctx)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return e.fn.Eval(values)
}
func (e aggScalarFunctionExpr) Type() reflect.Type { return e.fn.ResultType }
func (e aggScalarFunctionExpr) Nullable() bool     { return e.fn.Nullable }

type aggScalarSubqueryExpr struct {
	expr scalarSubqueryExpr
}

func (e aggScalarSubqueryExpr) Eval(ctx aggEvalContext) (any, error) {
	return e.expr.Eval(evalContext{row: ctx.row, outer: ctx.outer})
}
func (e aggScalarSubqueryExpr) Type() reflect.Type { return e.expr.Type() }
func (e aggScalarSubqueryExpr) Nullable() bool     { return e.expr.Nullable() }

func bindAggregateExpr(ctx *plannerContext, expr ast.Expr, schema []relationColumn, groupKeys map[string]struct{}, specs map[string]aggregateSpec) (aggregateEvalExpr, error) {
	switch expr := expr.(type) {
	case ast.PlaceholderExpr:
		bound, err := bindExpr(ctx, expr, schema)
		if err != nil {
			return nil, err
		}
		return aggLiteralExpr{value: ctx.args.positional[expr.Index], typ: bound.Type(), null: bound.Nullable()}, nil
	case ast.NamedPlaceholderExpr:
		bound, err := bindExpr(ctx, expr, schema)
		if err != nil {
			return nil, err
		}
		return aggLiteralExpr{value: ctx.args.named[normalizeName(expr.Name)], typ: bound.Type(), null: bound.Nullable()}, nil
	case ast.NumberLiteral:
		value, err := strconv.Atoi(expr.Raw)
		if err != nil {
			return nil, fmt.Errorf("invalid number literal %q", expr.Raw)
		}
		return aggLiteralExpr{value: value, typ: reflect.TypeFor[int](), null: false}, nil
	case ast.StringLiteral:
		return aggLiteralExpr{value: expr.Value, typ: reflect.TypeFor[string](), null: false}, nil
	case ast.BoolLiteral:
		return aggLiteralExpr{value: expr.Value, typ: reflect.TypeFor[bool](), null: false}, nil
	case ast.NullLiteral:
		return aggLiteralExpr{value: nil, typ: nil, null: true}, nil
	case ast.CallExpr:
		if expr.Star && !isAggregateName(expr.Name.Name) {
			return nil, fmt.Errorf("function %q does not accept *", expr.Name.Name)
		}
		if expr.Distinct && !isAggregateName(expr.Name.Name) {
			return nil, fmt.Errorf("function %q does not accept DISTINCT arguments", expr.Name.Name)
		}
		if !isAggregateName(expr.Name.Name) {
			fn, err := lookupScalarFunction(ctx.db, expr.Name.Name, len(expr.Args))
			if err != nil {
				return nil, err
			}
			args := make([]aggregateEvalExpr, 0, len(expr.Args))
			for _, arg := range expr.Args {
				bound, err := bindAggregateExpr(ctx, arg, schema, groupKeys, specs)
				if err != nil {
					return nil, err
				}
				args = append(args, bound)
			}
			return aggScalarFunctionExpr{fn: fn, args: args}, nil
		}
		key := exprFingerprint(expr)
		spec, ok := specs[key]
		if !ok {
			return nil, fmt.Errorf("unknown aggregate binding for %s", expr.Name.Name)
		}
		return aggResultExpr{key: key, typ: spec.typ, null: spec.nullable}, nil
	case ast.SubqueryExpr:
		bound, err := bindExpr(ctx, expr, schema)
		if err != nil {
			return nil, err
		}
		return aggScalarSubqueryExpr{expr: bound.(scalarSubqueryExpr)}, nil
	case ast.UnaryExpr:
		inner, err := bindAggregateExpr(ctx, expr.Expr, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		typ := inner.Type()
		if expr.Op == token.Not {
			typ = reflect.TypeFor[bool]()
		}
		return aggUnaryExpr{op: expr.Op, expr: inner, typ: typ, null: inner.Nullable()}, nil
	case ast.BinaryExpr:
		left, err := bindAggregateExpr(ctx, expr.Left, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		right, err := bindAggregateExpr(ctx, expr.Right, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		typ := left.Type()
		nullable := left.Nullable() || right.Nullable()
		switch expr.Op {
		case token.And, token.Or, token.Eq, token.NEq, token.Lt, token.LtE, token.Gt, token.GtE:
			typ = reflect.TypeFor[bool]()
			nullable = false
		case token.Plus, token.Minus, token.Star, token.Slash:
			typ = resultNumberType(left.Type(), right.Type())
		}
		return aggBinaryExpr{left: left, op: expr.Op, right: right, typ: typ, null: nullable}, nil
	case ast.IsExpr:
		left, err := bindAggregateExpr(ctx, expr.Left, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		right, err := bindAggregateExpr(ctx, expr.Right, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		return aggIsExpr{left: left, right: right, negated: expr.Negated}, nil
	case ast.InExpr:
		left, err := bindAggregateExpr(ctx, expr.Left, schema, groupKeys, specs)
		if err != nil {
			return nil, err
		}
		right := make([]aggregateEvalExpr, 0, len(expr.Right))
		for _, part := range expr.Right {
			bound, err := bindAggregateExpr(ctx, part, schema, groupKeys, specs)
			if err != nil {
				return nil, err
			}
			right = append(right, bound)
		}
		return aggInExpr{left: left, right: right, negated: expr.Negated}, nil
	default:
		if _, ok := groupKeys[exprFingerprint(expr)]; !ok {
			return nil, fmt.Errorf("expression must be aggregated or appear in GROUP BY")
		}
		bound, err := bindExpr(ctx, expr, schema)
		if err != nil {
			return nil, err
		}
		return aggRowExpr{expr: bound}, nil
	}
}

func collectAggregateSpecs(ctx *plannerContext, into map[string]aggregateSpec, expr ast.Expr, schema []relationColumn) error {
	switch expr := expr.(type) {
	case ast.CallExpr:
		if !isAggregateName(expr.Name.Name) {
			if expr.Star {
				return fmt.Errorf("function %q does not accept *", expr.Name.Name)
			}
			if expr.Distinct {
				return fmt.Errorf("function %q does not accept DISTINCT arguments", expr.Name.Name)
			}
			for _, arg := range expr.Args {
				if err := collectAggregateSpecs(ctx, into, arg, schema); err != nil {
					return err
				}
			}
			return nil
		}
		if expr.Star {
			if expr.Distinct {
				return fmt.Errorf("aggregate %s does not support DISTINCT *", expr.Name.Name)
			}
			if !strings.EqualFold(expr.Name.Name, "COUNT") {
				return fmt.Errorf("aggregate %s does not support *", expr.Name.Name)
			}
		} else if len(expr.Args) != 1 {
			return fmt.Errorf("aggregate %s requires exactly one argument", expr.Name.Name)
		}
		var boundArg boundExpr
		var err error
		if !expr.Star {
			arg := expr.Args[0]
			if containsAggregate(arg) {
				return fmt.Errorf("nested aggregate functions are not supported")
			}
			boundArg, err = bindExpr(ctx, arg, schema)
			if err != nil {
				return err
			}
		}
		key := exprFingerprint(expr)
		if _, exists := into[key]; exists {
			return nil
		}
		name := strings.ToUpper(expr.Name.Name)
		var argType reflect.Type
		if boundArg != nil {
			argType = boundArg.Type()
		}
		into[key] = aggregateSpec{
			key:      key,
			name:     name,
			arg:      boundArg,
			star:     expr.Star,
			distinct: expr.Distinct,
			typ:      aggregateResultType(name, argType),
			nullable: aggregateNullable(name),
		}
		return nil
	case ast.BinaryExpr:
		if err := collectAggregateSpecs(ctx, into, expr.Left, schema); err != nil {
			return err
		}
		return collectAggregateSpecs(ctx, into, expr.Right, schema)
	case ast.UnaryExpr:
		return collectAggregateSpecs(ctx, into, expr.Expr, schema)
	case ast.IsExpr:
		if err := collectAggregateSpecs(ctx, into, expr.Left, schema); err != nil {
			return err
		}
		return collectAggregateSpecs(ctx, into, expr.Right, schema)
	case ast.InExpr:
		if err := collectAggregateSpecs(ctx, into, expr.Left, schema); err != nil {
			return err
		}
		for _, part := range expr.Right {
			if err := collectAggregateSpecs(ctx, into, part, schema); err != nil {
				return err
			}
		}
	}
	return nil
}

func aggregateSpecsSorted(specs map[string]aggregateSpec) []aggregateSpec {
	out := make([]aggregateSpec, 0, len(specs))
	for _, spec := range specs {
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func containsAggregate(expr ast.Expr) bool {
	switch expr := expr.(type) {
	case ast.CallExpr:
		if isAggregateName(expr.Name.Name) {
			return true
		}
		for _, arg := range expr.Args {
			if containsAggregate(arg) {
				return true
			}
		}
		return false
	case ast.BinaryExpr:
		return containsAggregate(expr.Left) || containsAggregate(expr.Right)
	case ast.UnaryExpr:
		return containsAggregate(expr.Expr)
	case ast.IsExpr:
		return containsAggregate(expr.Left) || containsAggregate(expr.Right)
	case ast.InExpr:
		if containsAggregate(expr.Left) {
			return true
		}
		for _, part := range expr.Right {
			if containsAggregate(part) {
				return true
			}
		}
	case ast.SubqueryExpr:
		return false
	}
	return false
}

func selectsContainAggregate(items []ast.SelectItem) bool {
	for _, item := range items {
		if containsAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func orderTermsContainAggregate(items []ast.OrderTerm) bool {
	for _, item := range items {
		if containsAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func isAggregateName(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "MIN", "MAX", "AVG":
		return true
	default:
		return false
	}
}

func aggregateResultType(name string, argType reflect.Type) reflect.Type {
	switch name {
	case "COUNT":
		return reflect.TypeFor[int64]()
	case "AVG":
		return reflect.TypeFor[float64]()
	case "SUM":
		return resultNumberType(argType, argType)
	default:
		return argType
	}
}

func aggregateNullable(name string) bool {
	return name != "COUNT"
}

func newAggregateState(spec aggregateSpec) aggregateState {
	switch spec.name {
	case "COUNT":
		return &countAgg{arg: spec.arg, star: spec.star, distinct: spec.distinct}
	case "SUM":
		return &sumAgg{arg: spec.arg, distinct: spec.distinct}
	case "MIN":
		return &minMaxAgg{arg: spec.arg, max: false, distinct: spec.distinct}
	case "MAX":
		return &minMaxAgg{arg: spec.arg, max: true, distinct: spec.distinct}
	case "AVG":
		return &avgAgg{arg: spec.arg, distinct: spec.distinct}
	default:
		panic("unsupported aggregate: " + spec.name)
	}
}

type countAgg struct {
	arg      boundExpr
	star     bool
	distinct bool
	seen     distinctTracker
	count    int64
}

func (a *countAgg) Step(ctx evalContext) error {
	if a.star {
		a.count++
		return nil
	}
	value, err := a.arg.Eval(ctx)
	if err != nil {
		return err
	}
	if value != nil {
		if a.distinct && a.seen.Seen(value) {
			return nil
		}
		a.count++
	}
	return nil
}
func (a *countAgg) Result() any { return a.count }

type sumAgg struct {
	arg      boundExpr
	distinct bool
	seenVals distinctTracker
	seen     bool
	sumI     int64
	sumF     float64
	float    bool
}

func (a *sumAgg) Step(ctx evalContext) error {
	value, err := a.arg.Eval(ctx)
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	if a.distinct && a.seenVals.Seen(value) {
		return nil
	}
	if v, ok := asFloat64(value); ok {
		a.float = true
		a.sumF += v
		a.seen = true
		return nil
	}
	if v, ok := asInt64(value); ok {
		a.sumI += v
		a.seen = true
		return nil
	}
	return fmt.Errorf("SUM requires numeric values")
}
func (a *sumAgg) Result() any {
	if !a.seen {
		return nil
	}
	if a.float {
		return a.sumF
	}
	return a.sumI
}

type avgAgg struct {
	arg      boundExpr
	distinct bool
	seenVals distinctTracker
	sum      float64
	count    int64
}

func (a *avgAgg) Step(ctx evalContext) error {
	value, err := a.arg.Eval(ctx)
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	if a.distinct && a.seenVals.Seen(value) {
		return nil
	}
	if v, ok := asFloat64(value); ok {
		a.sum += v
		a.count++
		return nil
	}
	if v, ok := asInt64(value); ok {
		a.sum += float64(v)
		a.count++
		return nil
	}
	return fmt.Errorf("AVG requires numeric values")
}
func (a *avgAgg) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

type minMaxAgg struct {
	arg      boundExpr
	max      bool
	distinct bool
	seenVals distinctTracker
	seen     bool
	val      any
}

func (a *minMaxAgg) Step(ctx evalContext) error {
	value, err := a.arg.Eval(ctx)
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	if a.distinct && a.seenVals.Seen(value) {
		return nil
	}
	if !a.seen {
		a.val = value
		a.seen = true
		return nil
	}
	cmp, ok := compareValues(value, a.val)
	if !ok {
		return fmt.Errorf("MIN/MAX requires comparable values")
	}
	if (!a.max && cmp < 0) || (a.max && cmp > 0) {
		a.val = value
	}
	return nil
}
func (a *minMaxAgg) Result() any {
	if !a.seen {
		return nil
	}
	return a.val
}

func evaluateGroupKey(exprs []boundExpr, ctx evalContext) (string, error) {
	if len(exprs) == 0 {
		return "", nil
	}
	var b strings.Builder
	for i, expr := range exprs {
		value, err := expr.Eval(ctx)
		if err != nil {
			return "", err
		}
		if i > 0 {
			b.WriteString("|")
		}
		if !appendValueKey(&b, value) {
			return "", fmt.Errorf("unsupported GROUP BY key type %T", value)
		}
	}
	return b.String(), nil
}

type distinctTracker struct {
	keyed    map[string]struct{}
	fallback []any
}

func (t *distinctTracker) Seen(value any) bool {
	if key, ok := valueHashKey(value); ok {
		if t.keyed == nil {
			t.keyed = make(map[string]struct{})
		}
		if _, exists := t.keyed[key]; exists {
			return true
		}
		t.keyed[key] = struct{}{}
		return false
	}
	if seenValue(t.fallback, value) {
		return true
	}
	t.fallback = append(t.fallback, value)
	return false
}

func exprFingerprint(expr ast.Expr) string {
	switch expr := expr.(type) {
	case ast.Identifier:
		return "id:" + strings.ToLower(expr.Name)
	case ast.QualifiedRef:
		parts := make([]string, len(expr.Parts))
		for i, part := range expr.Parts {
			parts[i] = strings.ToLower(part.Name)
		}
		return "q:" + strings.Join(parts, ".")
	case ast.NumberLiteral:
		return "n:" + expr.Raw
	case ast.StringLiteral:
		return "s:" + expr.Value
	case ast.BoolLiteral:
		if expr.Value {
			return "b:true"
		}
		return "b:false"
	case ast.NullLiteral:
		return "null"
	case ast.PlaceholderExpr:
		return fmt.Sprintf("arg:%d", expr.Index)
	case ast.NamedPlaceholderExpr:
		return "named:" + strings.ToLower(expr.Name)
	case ast.UnaryExpr:
		return fmt.Sprintf("u:%d(%s)", expr.Op, exprFingerprint(expr.Expr))
	case ast.BinaryExpr:
		return fmt.Sprintf("b:%d(%s,%s)", expr.Op, exprFingerprint(expr.Left), exprFingerprint(expr.Right))
	case ast.IsExpr:
		return fmt.Sprintf("is:%t(%s,%s)", expr.Negated, exprFingerprint(expr.Left), exprFingerprint(expr.Right))
	case ast.InExpr:
		parts := make([]string, len(expr.Right))
		for i, part := range expr.Right {
			parts[i] = exprFingerprint(part)
		}
		return fmt.Sprintf("in:%t(%s:[%s])", expr.Negated, exprFingerprint(expr.Left), strings.Join(parts, ","))
	case ast.CallExpr:
		parts := make([]string, len(expr.Args))
		for i, arg := range expr.Args {
			parts[i] = exprFingerprint(arg)
		}
		return fmt.Sprintf("call:%s:%t:%t(%s)", strings.ToLower(expr.Name.Name), expr.Distinct, expr.Star, strings.Join(parts, ","))
	case ast.SubqueryExpr:
		return fmt.Sprintf("subq:%d:%d", expr.Span().Start, expr.Span().End)
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func lookupScalarFunction(db *DB, name string, argc int) (ScalarFunction, error) {
	fn, ok := db.functions[normalizeName(name)]
	if !ok {
		return ScalarFunction{}, fmt.Errorf("unknown function %q", name)
	}
	if argc < fn.MinArgs {
		return ScalarFunction{}, fmt.Errorf("function %q requires at least %d args", name, fn.MinArgs)
	}
	if fn.MaxArgs >= 0 && argc > fn.MaxArgs {
		return ScalarFunction{}, fmt.Errorf("function %q accepts at most %d args", name, fn.MaxArgs)
	}
	return fn, nil
}

func inferPathType(baseType reflect.Type, path []string) (reflect.Type, bool) {
	current := baseType
	nullable := false
	for _, part := range path {
		for current != nil && current.Kind() == reflect.Pointer {
			nullable = true
			current = current.Elem()
		}
		if current == nil {
			return nil, true
		}
		if current.Kind() == reflect.Interface {
			return nil, true
		}
		switch current.Kind() {
		case reflect.Struct:
			field, ok := findStructField(current, part)
			if !ok {
				return nil, true
			}
			current = field.Type
		case reflect.Map:
			if current.Key().Kind() != reflect.String {
				return nil, true
			}
			nullable = true
			current = current.Elem()
		default:
			return nil, true
		}
	}
	for current != nil && current.Kind() == reflect.Pointer {
		nullable = true
		current = current.Elem()
	}
	if current != nil && current.Kind() == reflect.Interface {
		return nil, true
	}
	return current, nullable
}

func traverseValuePath(value any, path []string) (any, error) {
	current := reflect.ValueOf(value)
	for _, part := range path {
		var ok bool
		current, ok = derefValue(current)
		if !ok {
			return nil, nil
		}
		switch current.Kind() {
		case reflect.Struct:
			field, found := findStructField(current.Type(), part)
			if !found {
				return nil, fmt.Errorf("unknown nested field %q on %s", part, current.Type())
			}
			current = current.FieldByIndex(field.Index)
		case reflect.Map:
			if current.Type().Key().Kind() != reflect.String {
				return nil, fmt.Errorf("nested map traversal requires string keys")
			}
			next := current.MapIndex(reflect.ValueOf(part))
			if !next.IsValid() {
				return nil, nil
			}
			current = next
		default:
			return nil, fmt.Errorf("cannot traverse %q on %s", part, current.Type())
		}
	}
	current, ok := derefValue(current)
	if !ok {
		return nil, nil
	}
	return current.Interface(), nil
}

func derefValue(value reflect.Value) (reflect.Value, bool) {
	current := value
	for current.IsValid() && (current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface) {
		if current.IsNil() {
			return reflect.Value{}, false
		}
		current = current.Elem()
	}
	if !current.IsValid() {
		return reflect.Value{}, false
	}
	return current, true
}

func findStructField(typ reflect.Type, name string) (reflect.StructField, bool) {
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tagName, include := parseColumnTag(field)
		if !include {
			continue
		}
		if strings.EqualFold(tagName, name) {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

func qualifiedRefName(parts []ast.Identifier) string {
	names := make([]string, len(parts))
	for i, part := range parts {
		names[i] = part.Name
	}
	return strings.Join(names, ".")
}

func truthy(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	default:
		return false
	}
}

func evalUnaryOp(op token.Type, value any) (any, error) {
	switch op {
	case token.Not:
		if value == nil {
			return nil, nil
		}
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires a boolean operand")
		}
		return !b, nil
	case token.Minus:
		if value == nil {
			return nil, nil
		}
		return negateNumber(value)
	default:
		return nil, fmt.Errorf("unsupported unary operator")
	}
}

func evalBinaryOp(op token.Type, left, right any) (any, error) {
	switch op {
	case token.And:
		return truthy(left) && truthy(right), nil
	case token.Or:
		return truthy(left) || truthy(right), nil
	case token.Eq:
		if left == nil || right == nil {
			return false, nil
		}
		cmp, ok := compareValues(left, right)
		return ok && cmp == 0, nil
	case token.NEq:
		if left == nil || right == nil {
			return false, nil
		}
		cmp, ok := compareValues(left, right)
		return ok && cmp != 0, nil
	case token.Lt:
		cmp, ok := compareValues(left, right)
		return ok && cmp < 0, nil
	case token.LtE:
		cmp, ok := compareValues(left, right)
		return ok && cmp <= 0, nil
	case token.Gt:
		cmp, ok := compareValues(left, right)
		return ok && cmp > 0, nil
	case token.GtE:
		cmp, ok := compareValues(left, right)
		return ok && cmp >= 0, nil
	case token.Plus, token.Minus, token.Star, token.Slash:
		return arithmetic(op, left, right)
	default:
		return nil, fmt.Errorf("unsupported binary operator")
	}
}

func evalIsOp(left, right any, negated bool) bool {
	var ok bool
	if right == nil {
		ok = left == nil
	} else {
		cmp, comparable := compareValues(left, right)
		ok = comparable && cmp == 0
	}
	if negated {
		ok = !ok
	}
	return ok
}

func evalInOp(left any, right []any, negated bool) bool {
	if left == nil {
		return false
	}
	matched := false
	for _, value := range right {
		cmp, ok := compareValues(left, value)
		if ok && cmp == 0 {
			matched = true
			break
		}
	}
	if negated {
		matched = !matched
	}
	return matched
}

func negateNumber(value any) (any, error) {
	if v, ok := asFloat64(value); ok {
		return -v, nil
	}
	if v, ok := asInt64(value); ok {
		return -v, nil
	}
	return nil, fmt.Errorf("unary minus requires a numeric operand")
}

func arithmetic(op token.Type, left, right any) (any, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	if lf, lok := asFloat64(left); lok {
		if rf, rok := asFloat64(right); rok {
			switch op {
			case token.Plus:
				return lf + rf, nil
			case token.Minus:
				return lf - rf, nil
			case token.Star:
				return lf * rf, nil
			case token.Slash:
				return lf / rf, nil
			}
		}
	}
	if li, lok := asInt64(left); lok {
		if ri, rok := asInt64(right); rok {
			switch op {
			case token.Plus:
				return li + ri, nil
			case token.Minus:
				return li - ri, nil
			case token.Star:
				return li * ri, nil
			case token.Slash:
				if ri == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return li / ri, nil
			}
		}
	}
	return nil, fmt.Errorf("operator %s requires numeric operands", tokenLabel(op))
}

func resultNumberType(left, right reflect.Type) reflect.Type {
	if isFloatType(left) || isFloatType(right) {
		return reflect.TypeFor[float64]()
	}
	return reflect.TypeFor[int64]()
}

func isFloatType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func compareValues(left, right any) (int, bool) {
	if left == nil || right == nil {
		return 0, false
	}
	if lt, lok := left.(time.Time); lok {
		rt, rok := right.(time.Time)
		if !rok {
			return 0, false
		}
		switch {
		case lt.Before(rt):
			return -1, true
		case lt.After(rt):
			return 1, true
		default:
			return 0, true
		}
	}
	if lf, lok := asFloat64(left); lok {
		if rf, rok := asFloat64(right); rok {
			switch {
			case lf < rf:
				return -1, true
			case lf > rf:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if li, lok := asInt64(left); lok {
		if ri, rok := asInt64(right); rok {
			switch {
			case li < ri:
				return -1, true
			case li > ri:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	switch l := left.(type) {
	case string:
		r, ok := right.(string)
		if !ok {
			return 0, false
		}
		switch {
		case l < r:
			return -1, true
		case l > r:
			return 1, true
		default:
			return 0, true
		}
	case bool:
		r, ok := right.(bool)
		if !ok {
			return 0, false
		}
		switch {
		case !l && r:
			return -1, true
		case l && !r:
			return 1, true
		default:
			return 0, true
		}
	default:
		if reflect.TypeOf(left) == reflect.TypeOf(right) && reflect.TypeOf(left).Comparable() {
			lv := reflect.ValueOf(left)
			rv := reflect.ValueOf(right)
			if lv.Interface() == rv.Interface() {
				return 0, true
			}
		}
		return 0, false
	}
}

func valuesEqual(left, right any) (bool, bool) {
	cmp, ok := compareValues(left, right)
	return ok && cmp == 0, ok
}

func seenValue(values []any, target any) bool {
	for _, existing := range values {
		if eq, ok := valuesEqual(existing, target); ok && eq {
			return true
		}
		if reflect.DeepEqual(existing, target) {
			return true
		}
	}
	return false
}

func rowHashKey(row Row) (string, bool) {
	var b strings.Builder
	b.Grow(len(row) * 16)
	for i, value := range row {
		if i > 0 {
			b.WriteByte('|')
		}
		if !appendValueKey(&b, value) {
			return "", false
		}
	}
	return b.String(), true
}

func valueHashKey(value any) (string, bool) {
	var b strings.Builder
	if !appendValueKey(&b, value) {
		return "", false
	}
	return b.String(), true
}

func appendValueKey(b *strings.Builder, value any) bool {
	return appendReflectValueKey(b, reflect.ValueOf(value))
}

func appendReflectValueKey(b *strings.Builder, value reflect.Value) bool {
	if !value.IsValid() {
		b.WriteString("nil")
		return true
	}
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			b.WriteString("nil")
			return true
		}
		b.WriteString("*")
		value = value.Elem()
	}

	if value.CanInterface() {
		if t, ok := value.Interface().(time.Time); ok {
			b.WriteString("time:")
			b.WriteString(strconv.FormatInt(t.UnixNano(), 10))
			return true
		}
	}

	switch value.Kind() {
	case reflect.Bool:
		b.WriteString("b:")
		if value.Bool() {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
		return true
	case reflect.String:
		b.WriteString("s:")
		b.WriteString(value.String())
		return true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(value.Int(), 10))
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(value.Uint(), 10))
		return true
	case reflect.Float32, reflect.Float64:
		b.WriteString("f:")
		b.WriteString(strconv.FormatUint(math.Float64bits(value.Convert(reflect.TypeFor[float64]()).Float()), 16))
		return true
	case reflect.Slice, reflect.Array:
		b.WriteString("a[")
		for i := range value.Len() {
			if i > 0 {
				b.WriteByte(',')
			}
			if !appendReflectValueKey(b, value.Index(i)) {
				return false
			}
		}
		b.WriteByte(']')
		return true
	case reflect.Map:
		b.WriteString("m{")
		type pair struct {
			key string
			val reflect.Value
		}
		pairs := make([]pair, 0, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			key, ok := valueHashKey(iter.Key().Interface())
			if !ok {
				return false
			}
			pairs = append(pairs, pair{key: key, val: iter.Value()})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
		for i, item := range pairs {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(item.key)
			b.WriteByte(':')
			if !appendReflectValueKey(b, item.val) {
				return false
			}
		}
		b.WriteByte('}')
		return true
	case reflect.Struct:
		b.WriteString("struct:")
		b.WriteString(value.Type().String())
		b.WriteByte('{')
		for i := range value.NumField() {
			if i > 0 {
				b.WriteByte(',')
			}
			field := value.Type().Field(i)
			b.WriteString(field.Name)
			b.WriteByte(':')
			if !appendReflectValueKey(b, value.Field(i)) {
				return false
			}
		}
		b.WriteByte('}')
		return true
	default:
		if value.CanInterface() && value.Type().Comparable() {
			b.WriteString("c:")
			b.WriteString(value.Type().String())
			b.WriteByte(':')
			b.WriteString(fmt.Sprint(value.Interface()))
			return true
		}
		return false
	}
}

func compareOrderKeys(keys [][]any, terms []orderTermPlan, leftIdx, rightIdx int) int {
	for termIdx, term := range terms {
		cmp, ok := compareValues(keys[termIdx][leftIdx], keys[termIdx][rightIdx])
		if !ok || cmp == 0 {
			continue
		}
		if term.desc {
			return -cmp
		}
		return cmp
	}
	return 0
}

type topNIndexHeap struct {
	indices []int
	keys    [][]any
	terms   []orderTermPlan
}

func (h topNIndexHeap) Len() int { return len(h.indices) }

func (h topNIndexHeap) Less(i, j int) bool {
	return compareOrderKeys(h.keys, h.terms, h.indices[i], h.indices[j]) > 0
}

func (h topNIndexHeap) Swap(i, j int) {
	h.indices[i], h.indices[j] = h.indices[j], h.indices[i]
}

func (h *topNIndexHeap) Push(x any) {
	h.indices = append(h.indices, x.(int))
}

func (h *topNIndexHeap) Pop() any {
	last := len(h.indices) - 1
	item := h.indices[last]
	h.indices = h.indices[:last]
	return item
}

func topNIndices(all []int, limit int, keys [][]any, terms []orderTermPlan) []int {
	h := &topNIndexHeap{
		indices: make([]int, 0, limit),
		keys:    keys,
		terms:   terms,
	}
	for _, idx := range all {
		if h.Len() < limit {
			heap.Push(h, idx)
			continue
		}
		if compareOrderKeys(keys, terms, idx, h.indices[0]) < 0 {
			h.indices[0] = idx
			heap.Fix(h, 0)
		}
	}
	out := append([]int(nil), h.indices...)
	sort.SliceStable(out, func(i, j int) bool {
		return compareOrderKeys(keys, terms, out[i], out[j]) < 0
	})
	return out
}

func asInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

func asFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func tokenLabel(op token.Type) string {
	switch op {
	case token.Plus:
		return "+"
	case token.Minus:
		return "-"
	case token.Star:
		return "*"
	case token.Slash:
		return "/"
	default:
		return "operator"
	}
}
