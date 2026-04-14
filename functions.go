package structql

import (
	"fmt"
	"reflect"
	"strings"
)

type ScalarFunction struct {
	MinArgs    int
	MaxArgs    int
	ResultType reflect.Type
	Nullable   bool
	Eval       func(args []any) (any, error)
}

func (db *DB) RegisterFunction(name string, fn ScalarFunction) error {
	key := normalizeName(name)
	if key == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if isAggregateName(name) {
		return fmt.Errorf("function name %q is reserved for aggregates", name)
	}
	if fn.Eval == nil {
		return fmt.Errorf("function %q must define Eval", name)
	}
	if fn.MinArgs < 0 {
		return fmt.Errorf("function %q cannot require a negative arg count", name)
	}
	if fn.MaxArgs >= 0 && fn.MaxArgs < fn.MinArgs {
		return fmt.Errorf("function %q has invalid arg bounds", name)
	}
	if _, exists := db.functions[key]; exists {
		return fmt.Errorf("function %q already registered", name)
	}
	db.functions[key] = fn
	return nil
}

func (db *DB) registerBuiltinFunctions() {
	_ = db.RegisterFunction("len", ScalarFunction{
		MinArgs:    1,
		MaxArgs:    1,
		ResultType: reflect.TypeFor[int64](),
		Nullable:   true,
		Eval: func(args []any) (any, error) {
			if args[0] == nil {
				return nil, nil
			}
			value := reflect.ValueOf(args[0])
			for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
				if value.IsNil() {
					return nil, nil
				}
				value = value.Elem()
			}
			if !value.IsValid() {
				return nil, nil
			}
			switch value.Kind() {
			case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
				return int64(value.Len()), nil
			default:
				return nil, fmt.Errorf("len requires a string, array, slice, or map")
			}
		},
	})
	_ = db.RegisterFunction("contains", ScalarFunction{
		MinArgs:    2,
		MaxArgs:    2,
		ResultType: reflect.TypeFor[bool](),
		Nullable:   true,
		Eval: func(args []any) (any, error) {
			container := args[0]
			needle := args[1]
			if container == nil {
				return nil, nil
			}
			value := reflect.ValueOf(container)
			for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
				if value.IsNil() {
					return nil, nil
				}
				value = value.Elem()
			}
			if !value.IsValid() {
				return nil, nil
			}
			switch value.Kind() {
			case reflect.String:
				part, ok := needle.(string)
				if !ok {
					return nil, fmt.Errorf("contains on strings requires a string needle")
				}
				return strings.Contains(value.String(), part), nil
			case reflect.Array, reflect.Slice:
				for i := range value.Len() {
					item := value.Index(i).Interface()
					if eq, ok := valuesEqual(item, needle); ok && eq {
						return true, nil
					}
					if reflect.DeepEqual(item, needle) {
						return true, nil
					}
				}
				return false, nil
			default:
				return nil, fmt.Errorf("contains requires a string, array, or slice")
			}
		},
	})
}
