package structql

import "fmt"

type NamedArg struct {
	Name  string
	Value any
}

func Named(name string, value any) NamedArg {
	return NamedArg{Name: name, Value: value}
}

type queryArgs struct {
	positional []any
	usedPos    map[int]struct{}
	named      map[string]any
	usedNamed  map[string]struct{}
}

func parseQueryArgs(args []any) (*queryArgs, error) {
	out := &queryArgs{
		positional: make([]any, 0, len(args)),
		usedPos:    make(map[int]struct{}),
		named:      make(map[string]any),
		usedNamed:  make(map[string]struct{}),
	}

	seenNamed := false
	for i, arg := range args {
		named, ok := arg.(NamedArg)
		if !ok {
			if seenNamed {
				return nil, fmt.Errorf("positional query arg %d appears after named args", i+1)
			}
			out.positional = append(out.positional, arg)
			continue
		}

		seenNamed = true
		key := normalizeName(named.Name)
		if key == "" {
			return nil, fmt.Errorf("named query arg cannot have an empty name")
		}
		if _, exists := out.named[key]; exists {
			return nil, fmt.Errorf("duplicate named query arg %q", named.Name)
		}
		out.named[key] = named.Value
	}

	return out, nil
}
