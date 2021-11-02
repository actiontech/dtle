package element

const (
	IdentifierTypeQuoted    = iota // "schema" . "table"
	IdentifierTypeNonQuoted        // schema . table
)

type Identifier struct {
	Typ   int
	Value string
}

type NumberOrAsterisk struct {
	Number 		int
	IsAsterisk 	bool
}