package structql

import (
	"fmt"
	"reflect"
	"strings"
)

type resultField struct {
	name  string
	index []int
	typ   reflect.Type
}

func (r *Result) Scan(dest any) error {
	if r == nil {
		return fmt.Errorf("nil result")
	}
	if dest == nil {
		return fmt.Errorf("destination cannot be nil")
	}

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Pointer || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer to a slice")
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("destination must point to a slice, got %s", sliceValue.Kind())
	}

	elemType := sliceValue.Type().Elem()
	elemIsPointer := elemType.Kind() == reflect.Pointer
	structType, err := resultStructType(elemType)
	if err != nil {
		return err
	}

	fields := resultFields(structType)
	byName := make(map[string]resultField, len(fields))
	for _, field := range fields {
		byName[normalizeName(field.name)] = field
	}

	assignments := make([]resultField, len(r.Columns))
	for i, col := range r.Columns {
		field, ok := byName[normalizeName(col.Name)]
		if !ok {
			return fmt.Errorf("result column %q has no matching destination field", col.Name)
		}
		assignments[i] = field
	}

	out := reflect.MakeSlice(sliceValue.Type(), 0, len(r.Rows))
	for rowIdx, row := range r.Rows {
		if len(row) != len(r.Columns) {
			return fmt.Errorf("row %d has %d values for %d columns", rowIdx, len(row), len(r.Columns))
		}

		elem := reflect.New(structType).Elem()
		for colIdx, value := range row {
			if err := assignResultValue(elem.FieldByIndex(assignments[colIdx].index), value, r.Columns[colIdx].Name); err != nil {
				return fmt.Errorf("row %d: %w", rowIdx, err)
			}
		}

		if elemIsPointer {
			ptr := reflect.New(structType)
			ptr.Elem().Set(elem)
			out = reflect.Append(out, ptr)
		} else {
			out = reflect.Append(out, elem)
		}
	}

	sliceValue.Set(out)
	return nil
}

func resultStructType(elemType reflect.Type) (reflect.Type, error) {
	if elemType.Kind() == reflect.Pointer {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("destination slice must contain structs or *struct, got %s", elemType)
	}
	return elemType, nil
}

func resultFields(structType reflect.Type) []resultField {
	var fields []resultField
	for field := range structType.Fields() {
		if field.PkgPath != "" {
			continue
		}

		name, include := parseColumnTag(field)
		if !include {
			continue
		}

		fields = append(fields, resultField{
			name:  name,
			index: field.Index,
			typ:   field.Type,
		})
	}
	return fields
}

func assignResultValue(field reflect.Value, value any, columnName string) error {
	if !field.CanSet() {
		return fmt.Errorf("field for column %q is not settable", columnName)
	}

	if value == nil {
		if field.Kind() == reflect.Pointer {
			field.Set(reflect.Zero(field.Type()))
			return nil
		}
		return fmt.Errorf("cannot assign NULL to non-pointer field for column %q", columnName)
	}

	valueRef := reflect.ValueOf(value)
	if field.Kind() == reflect.Pointer {
		elemType := field.Type().Elem()
		assigned, err := coerceValue(valueRef, elemType, columnName)
		if err != nil {
			return err
		}
		ptr := reflect.New(elemType)
		ptr.Elem().Set(assigned)
		field.Set(ptr)
		return nil
	}

	assigned, err := coerceValue(valueRef, field.Type(), columnName)
	if err != nil {
		return err
	}
	field.Set(assigned)
	return nil
}

func coerceValue(value reflect.Value, target reflect.Type, columnName string) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, fmt.Errorf("invalid value for column %q", columnName)
	}
	if value.Type().AssignableTo(target) {
		return value, nil
	}
	if value.Type().ConvertibleTo(target) {
		return value.Convert(target), nil
	}
	return reflect.Value{}, fmt.Errorf("cannot assign %s to %s for column %q", value.Type(), target, columnName)
}

func (r *Result) columnIndex(name string) int {
	if r == nil {
		return -1
	}
	normalized := normalizeName(name)
	for i, col := range r.Columns {
		if strings.EqualFold(normalized, normalizeName(col.Name)) {
			return i
		}
	}
	return -1
}
