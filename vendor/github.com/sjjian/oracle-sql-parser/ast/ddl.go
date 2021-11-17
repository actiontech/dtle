package ast

import (
	"github.com/sjjian/oracle-sql-parser/ast/element"
)

/*
	Alter Table  Statement
    see: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ALTER-TABLE.html#GUID-552E7373-BF93-477D-9DA3-B2C9386F2877
*/

type AlterTableStmt struct {
	node
	TableName         *TableName
	AlterTableClauses []AlterTableClause
}

type AlterTableClause interface {
	IsAlterTableClause()
}

type alterTableClause struct{}

func (c *alterTableClause) IsAlterTableClause() {}

type AddColumnClause struct {
	alterTableClause
	Columns []*ColumnDef
}

type ModifyColumnClause struct {
	alterTableClause
	Columns []*ColumnDef
}

type DropColumnClause struct {
	alterTableClause
	Type       DropColumnType
	Columns    []*element.Identifier
	Props      []DropColumnProp
	CheckPoint *int
}

type RenameColumnClause struct {
	alterTableClause
	OldName *element.Identifier
	NewName *element.Identifier
}

type AddConstraintClause struct {
	alterTableClause
}

type ModifyConstraintClause struct {
	alterTableClause
}

type RenameConstraintClause struct {
	alterTableClause
}

type DropConstraintClause struct {
	alterTableClause
}

/*
	Create Table  Statement
	see: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-TABLE.html#GUID-F9CE0CC3-13AE-4744-A43C-EAC7A71AAAB6
*/

type CreateTableStmt struct {
	node
	TableName *TableName
	RelTable  *RelTableDef
}

type RelTableDef struct {
	TableStructs []TableStructDef
}

type TableStructDef interface {
	IsTableStructDef()
}

type tableStructDef struct{}

func (c *tableStructDef) IsTableStructDef() {}

type ColumnDef struct {
	tableStructDef
	ColumnName  *element.Identifier
	Datatype    element.Datatype
	Collation   *Collation
	Props       []ColumnProp
	Default     *ColumnDefault
	Constraints []*InlineConstraint
}

type InlineConstraint struct {
	Name *element.Identifier
	Type ConstraintType
}

type OutOfLineConstraint struct {
	tableStructDef
	InlineConstraint
	Columns []*element.Identifier
}

/*
	Create Index  Statement
	see: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-INDEX.html#GUID-1F89BBC0-825F-4215-AF71-7588E31D8BFE
*/

type CreateIndexStmt struct {
	node
}

/*
	Drop Table  Statement
	see: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-INDEX.html#GUID-1F89BBC0-825F-4215-AF71-7588E31D8BFE
*/
type DropTableStmt struct {
	node
	TableName *TableName
}