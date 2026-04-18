package structql

import (
	"reflect"
	"sync"
)

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
	schema       []Column
	columns      []tableColumn
	rows         int
	rowCacheOnce sync.Once
	rowCache     []Row
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

func (t *Table) materializedRows() []Row {
	if t == nil {
		return nil
	}
	t.rowCacheOnce.Do(func() {
		rows := make([]Row, t.rows)
		for i := 0; i < t.rows; i++ {
			row := make(Row, len(t.columns))
			for j, col := range t.columns {
				row[j] = col.ValueAt(i)
			}
			rows[i] = row
		}
		t.rowCache = rows
	})
	return t.rowCache
}

type DB struct {
	tables     map[string]*Table
	functions  map[string]ScalarFunction
	cacheMu    sync.RWMutex
	queryCache map[string]*PreparedQuery
}

func NewDB() *DB {
	db := &DB{
		tables:     make(map[string]*Table),
		functions:  make(map[string]ScalarFunction),
		queryCache: make(map[string]*PreparedQuery),
	}
	db.registerBuiltinFunctions()
	return db
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

type anyColumn struct {
	def  Column
	data []any
}

func (c anyColumn) Len() int {
	return len(c.data)
}

func (c anyColumn) ValueAt(i int) any {
	return c.data[i]
}

func (c anyColumn) Column() Column {
	return c.def
}
