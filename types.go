package structql

import "reflect"

type Column struct {
	Name     string
	Type     reflect.Type
	Nullable bool
}

type ResultColumn struct {
	Name     string
	Type     reflect.Type
	Nullable bool
}

type Row []any

type Result struct {
	Columns []ResultColumn
	Rows    []Row
}

type Table struct {
	schema  []Column
	columns []tableColumn
	rows    int
}

func (t *Table) Schema() []Column {
	out := make([]Column, len(t.schema))
	copy(out, t.schema)
	return out
}

func (t *Table) Len() int {
	if t == nil {
		return 0
	}
	return t.rows
}

type DB struct {
	tables map[string]*Table
}

func NewDB() *DB {
	return &DB{tables: make(map[string]*Table)}
}

type tableColumn interface {
	Len() int
	ValueAt(int) any
	Column() Column
}

type sliceColumn[T any] struct {
	def  Column
	data []T
}

func (c sliceColumn[T]) Len() int {
	return len(c.data)
}

func (c sliceColumn[T]) ValueAt(i int) any {
	return c.data[i]
}

func (c sliceColumn[T]) Column() Column {
	return c.def
}

type nullableColumn[T any] struct {
	def   Column
	data  []T
	valid []bool
}

func (c nullableColumn[T]) Len() int {
	return len(c.data)
}

func (c nullableColumn[T]) ValueAt(i int) any {
	if !c.valid[i] {
		return nil
	}
	return c.data[i]
}

func (c nullableColumn[T]) Column() Column {
	return c.def
}
