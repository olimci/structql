package ast

type Span struct {
	Start int
	End   int
}

func MergeSpan(a, b Span) Span {
	return Span{Start: a.Start, End: b.End}
}
