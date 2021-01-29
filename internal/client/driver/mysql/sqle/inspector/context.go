package inspector

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

type TableInfo struct {
	Size     float64
	sizeLoad bool

	// OriginalTable save parser object from db by query "show create table ...";
	// using in inspect and generate rollback sql
	OriginalTable *ast.CreateTableStmt

	//
	MergedTable *ast.CreateTableStmt

	// save alter table parse object from input sql;
	AlterTables []*ast.AlterTableStmt
}

type SchemaInfo struct {
	DefaultEngine    string
	engineLoad       bool
	DefaultCharacter string
	characterLoad    bool
	Tables           map[string]*TableInfo
}

type Context struct {
	// currentSchema will change after sql "use database"
	currentSchema string

	schemas map[string]*SchemaInfo
	// if schemas info has collected, set true
	schemaHasLoad bool
}

func NewContext(parent *Context) *Context {
	ctx := &Context{
		schemas: map[string]*SchemaInfo{},
	}
	if parent == nil {
		return ctx
	}
	ctx.schemaHasLoad = parent.schemaHasLoad
	ctx.currentSchema = parent.currentSchema
	for schemaName, schema := range parent.schemas {
		newSchema := &SchemaInfo{
			Tables: map[string]*TableInfo{},
		}
		if schema == nil || schema.Tables == nil {
			continue
		}
		for tableName, table := range schema.Tables {
			newSchema.Tables[tableName] = &TableInfo{
				Size:          table.Size,
				sizeLoad:      table.sizeLoad,
				OriginalTable: table.OriginalTable,
				MergedTable:   table.MergedTable,
				AlterTables:   table.AlterTables,
			}
		}
		ctx.schemas[schemaName] = newSchema
	}
	return ctx
}

func (c *Context) HasLoadSchemas() bool {
	return c.schemaHasLoad
}

func (c *Context) SetSchemasLoad() {
	c.schemaHasLoad = true
}

func (c *Context) LoadSchemas(schemas []string) {
	if c.HasLoadSchemas() {
		return
	}
	for _, schema := range schemas {
		c.schemas[schema] = &SchemaInfo{}
	}
	c.SetSchemasLoad()
}

func (c *Context) GetSchema(schemaName string) (*SchemaInfo, bool) {
	schema, has := c.schemas[schemaName]
	return schema, has
}

func (c *Context) HasSchema(schemaName string) (has bool) {
	_, has = c.GetSchema(schemaName)
	return
}

func (c *Context) AddSchema(name string) {
	if c.HasSchema(name) {
		return
	}
	c.schemas[name] = &SchemaInfo{}
}

func (c *Context) DelSchema(name string) {
	delete(c.schemas, name)
}

func (c *Context) HasLoadTables(schemaName string) (hasLoad bool) {
	if schema, ok := c.GetSchema(schemaName); ok {
		if schema.Tables == nil {
			hasLoad = false
		} else {
			hasLoad = true
		}
	}
	return
}

func (c *Context) LoadTables(schemaName string, tablesName []string) {
	schema, ok := c.GetSchema(schemaName)
	if !ok {
		return
	}
	if c.HasLoadTables(schemaName) {
		return
	}
	schema.Tables = map[string]*TableInfo{}
	for _, name := range tablesName {
		schema.Tables[name] = &TableInfo{
			AlterTables: []*ast.AlterTableStmt{},
		}
	}
}

func (c *Context) GetTable(schemaName, tableName string) (*TableInfo, bool) {
	schema, SchemaExist := c.GetSchema(schemaName)
	if !SchemaExist {
		return nil, false
	}
	if !c.HasLoadTables(schemaName) {
		return nil, false
	}
	table, tableExist := schema.Tables[tableName]
	return table, tableExist
}

func (c *Context) HasTable(schemaName, tableName string) (has bool) {
	_, has = c.GetTable(schemaName, tableName)
	return
}

func (c *Context) AddTable(schemaName, tableName string, table *TableInfo) {
	schema, exist := c.GetSchema(schemaName)
	if !exist {
		return
	}
	if !c.HasLoadTables(schemaName) {
		return
	}
	schema.Tables[tableName] = table
}

func (c *Context) DelTable(schemaName, tableName string) {
	schema, exist := c.GetSchema(schemaName)
	if !exist {
		return
	}
	delete(schema.Tables, tableName)
}

func (c *Context) UseSchema(schema string) {
	c.currentSchema = schema
}

func (ctx *Context) UpdateContext(node ast.Node, dbtype string) {
	switch s := node.(type) {
	case *ast.UseStmt:
		// change current schema
		if ctx.HasSchema(s.DBName) {
			ctx.UseSchema(s.DBName)
		}
	case *ast.CreateDatabaseStmt:
		if ctx.HasLoadSchemas() {
			ctx.AddSchema(s.Name)
		}
	case *ast.CreateTableStmt:
		schemaName := ctx.getSchemaName(s.Table)
		tableName := s.Table.Name.O
		if ctx.HasTable(schemaName, tableName) {
			return
		}
		ctx.AddTable(schemaName, tableName,
			&TableInfo{
				Size:          0, // table is empty after create
				sizeLoad:      true,
				OriginalTable: s,
				AlterTables:   []*ast.AlterTableStmt{},
			})
	case *ast.DropDatabaseStmt:
		if ctx.HasLoadSchemas() {
			ctx.DelSchema(s.Name)
		}
	case *ast.DropTableStmt:
		if ctx.HasLoadSchemas() {
			for _, table := range s.Tables {
				schemaName := ctx.getSchemaName(table)
				tableName := table.Name.O
				if ctx.HasTable(schemaName, tableName) {
					ctx.DelTable(schemaName, tableName)
				}
			}
		}

	case *ast.AlterTableStmt:
		info, exist := ctx.getTableInfo(s.Table)
		if exist {
			var oldTable *ast.CreateTableStmt
			var err error
			if info.MergedTable != nil {
				oldTable = info.MergedTable
			} else if info.OriginalTable != nil {
				oldTable, err = ParseCreateTableStmt(dbtype, info.OriginalTable.Text())
				if err != nil {
					return
				}
			}
			info.MergedTable, _ = mergeAlterToTable(oldTable, s)
			info.AlterTables = append(info.AlterTables, s)
			// rename table
			if s.Table.Name.O != info.MergedTable.Table.Name.O {
				schemaName := ctx.getSchemaName(s.Table)
				ctx.DelTable(schemaName, s.Table.Name.O)
				ctx.AddTable(schemaName, info.MergedTable.Table.Name.O, info)
			}
		}
	case *ast.RenameTableStmt:
		for _, tt := range s.TableToTables {
			info, exist := ctx.getTableInfo(tt.OldTable)
			if exist {
				var err error
				if info.MergedTable == nil {
					if info.OriginalTable != nil {
						info.MergedTable, err = ParseCreateTableStmt(dbtype, info.OriginalTable.Text())
						if err != nil {
							return
						}
					} else {
						// A bug
						return
					}
				}

				OldSchemaName := ctx.getSchemaName(tt.OldTable)
				NewSchemaName := ctx.getSchemaName(tt.NewTable)

				// Do not use tt.NewTable.Schema. It might be empty.
				info.MergedTable.Table.Schema = model.NewCIStr(NewSchemaName)
				info.MergedTable.Table.Name = tt.NewTable.Name

				// TODO s is not an AlterTableStmt
				//  plan: transform `rename table` to `alter table rename`
				//info.AlterTables = append(info.AlterTables, s)

				// Note: it is valid to `rename table from a.a to b.a`.
				ctx.DelTable(OldSchemaName, tt.OldTable.Name.O)
				ctx.AddTable(NewSchemaName, info.MergedTable.Table.Name.O, info)
			}
		}
	default:
	}
}

func (c *Context) getSchemaName(stmt *ast.TableName) string {
	if stmt.Schema.String() == "" {
		return c.currentSchema
	} else {
		return stmt.Schema.String()
	}
}

func (c *Context) getTableInfo(stmt *ast.TableName) (*TableInfo, bool) {
	schema := c.getSchemaName(stmt)
	table := stmt.Name.String()
	return c.GetTable(schema, table)
}
