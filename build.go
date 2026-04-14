package structql

import (
	"fmt"
	"reflect"
	"strings"
)

const structTagName = "structql"

type fieldSpec struct {
	name     string
	index    []int
	typ      reflect.Type
	nullable bool
}

func BuildTable[T any](rows []T) (*Table, error) {
	rowType := reflect.TypeFor[T]()
	structType, pointerRows, err := tableStructType(rowType)
	if err != nil {
		return nil, err
	}

	fields := tableFields(structType)
	schema := make([]Column, 0, len(fields))
	cols := make([]tableColumn, 0, len(fields))
	rowsVal := reflect.ValueOf(rows)

	for _, field := range fields {
		col := Column{Name: field.name, Type: field.typ, Nullable: field.nullable}
		built, err := buildColumn(col, field, rowsVal, pointerRows)
		if err != nil {
			return nil, err
		}
		schema = append(schema, col)
		cols = append(cols, built)
	}

	return &Table{
		schema:  schema,
		columns: cols,
		rows:    len(rows),
	}, nil
}

func tableStructType(rowType reflect.Type) (reflect.Type, bool, error) {
	if rowType.Kind() == reflect.Pointer {
		if rowType.Elem().Kind() != reflect.Struct {
			return nil, false, fmt.Errorf("BuildTable requires a struct or *struct element type, got %s", rowType)
		}
		return rowType.Elem(), true, nil
	}
	if rowType.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("BuildTable requires a struct or *struct element type, got %s", rowType)
	}
	return rowType, false, nil
}

func tableFields(structType reflect.Type) []fieldSpec {
	var fields []fieldSpec
	for field := range structType.Fields() {
		if field.PkgPath != "" {
			continue
		}

		name, include := parseColumnTag(field)
		if !include {
			continue
		}

		typ := field.Type
		nullable := typ.Kind() == reflect.Pointer
		if nullable {
			typ = typ.Elem()
		}

		fields = append(fields, fieldSpec{
			name:     name,
			index:    field.Index,
			typ:      typ,
			nullable: nullable,
		})
	}
	return fields
}

func parseColumnTag(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get(structTagName)
	if tag == "-" {
		return "", false
	}
	if tag == "" {
		return field.Name, true
	}
	name := strings.TrimSpace(tag)
	if name == "" {
		return field.Name, true
	}
	return name, true
}

func buildColumn(def Column, field fieldSpec, rows reflect.Value, pointerRows bool) (tableColumn, error) {
	switch field.typ.Kind() {
	case reflect.Bool:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) bool { return v.Bool() })
	case reflect.String:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) string { return v.String() })
	case reflect.Int:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) int { return int(v.Int()) })
	case reflect.Int8:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) int8 { return int8(v.Int()) })
	case reflect.Int16:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) int16 { return int16(v.Int()) })
	case reflect.Int32:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) int32 { return int32(v.Int()) })
	case reflect.Int64:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) int64 { return v.Int() })
	case reflect.Uint:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) uint { return uint(v.Uint()) })
	case reflect.Uint8:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) uint8 { return uint8(v.Uint()) })
	case reflect.Uint16:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) uint16 { return uint16(v.Uint()) })
	case reflect.Uint32:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) uint32 { return uint32(v.Uint()) })
	case reflect.Uint64:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) uint64 { return v.Uint() })
	case reflect.Float32:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) float32 { return float32(v.Float()) })
	case reflect.Float64:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) float64 { return v.Float() })
	default:
		return buildTypedColumn(def, field, rows, pointerRows, func(v reflect.Value) any { return v.Interface() })
	}
}

func buildTypedColumn[T any](def Column, field fieldSpec, rows reflect.Value, pointerRows bool, extract func(reflect.Value) T) (tableColumn, error) {
	data := make([]T, rows.Len())
	if field.nullable {
		valid := make([]bool, rows.Len())
		for i := range rows.Len() {
			fv, err := tableFieldValue(rows.Index(i), field.index, pointerRows)
			if err != nil {
				return nil, err
			}
			if fv.IsNil() {
				continue
			}
			valid[i] = true
			data[i] = extract(fv.Elem())
		}
		return nullableColumn[T]{def: def, data: data, valid: valid}, nil
	}

	for i := range rows.Len() {
		fv, err := tableFieldValue(rows.Index(i), field.index, pointerRows)
		if err != nil {
			return nil, err
		}
		data[i] = extract(fv)
	}
	return sliceColumn[T]{def: def, data: data}, nil
}

func tableFieldValue(row reflect.Value, index []int, pointerRows bool) (reflect.Value, error) {
	if pointerRows {
		if row.IsNil() {
			return reflect.Value{}, fmt.Errorf("BuildTable does not allow nil row pointers")
		}
		row = row.Elem()
	}
	return row.FieldByIndex(index), nil
}
