package ast

import (
	"github.com/sjjian/oracle-sql-parser/ast/element"
)

type TableName struct {
	Schema *element.Identifier
	Table  *element.Identifier
}

type Collation struct {
	Name *element.Identifier
}

type ColumnProp int

const (
	ColumnPropEmpty ColumnProp = iota
	ColumnPropSort             // for add column
	ColumnPropInvisible
	ColumnPropVisible
	ColumnPropSubstitutable         // for modify column
	ColumnPropNotSubstitutable      // for modify column
	ColumnPropSubstitutableForce    // for modify column
	ColumnPropNotSubstitutableForce // for modify column
)

type ColumnDefault struct {
	OnNull bool
	Value  interface{}
}

type ConstraintType int

const (
	ConstraintTypeNull ConstraintType = iota
	ConstraintTypeNotNull
	ConstraintTypeUnique
	ConstraintTypePK
	ConstraintTypeReferences
)

//type ConstraintState int

//const (
//	ConstraintStateDeferrable ConstraintState = iota
//	ConstraintStateNotDeferrable
//	ConstraintStateInitiallyDeferred
//	ConstraintStateInitiallyImmediate
//	ConstraintStateRely
//	ConstraintStateNorely
//)

type DropColumnType int

const (
	DropColumnTypeDrop DropColumnType = iota
	DropColumnTypeSetUnused
	DropColumnTypeDropUnusedColumns
	DropColumnTypeDropColumnsContinue
)

type DropColumnProp int

const (
	DropColumnPropEmpty DropColumnProp = iota
	DropColumnPropCascade
	DropColumnPropInvalidate
	DropColumnPropOnline
)
