package main

import (
	"bytes"
	"github.com/actiontech/dtle/helper/u"
	tparser "github.com/pingcap/tidb/parser"
	tast "github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"

	"fmt"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"log"
)

func main() {
	fmt.Printf("")
	log.Println("hello")

	p := tparser.New()
	sql := "CREATE DATABASE `db1` /*!40100 DEFAULT CHARACTER SET utf8 */"
	ast, err := p.ParseOneStmt(sql, "", "")

	u.PanicIfErr(err)

	buf := bytes.NewBuffer(nil)
	rCtx := &format.RestoreCtx{
		//Flags:     format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes | format.RestoreSpacesAroundBinaryOperation,
		Flags:     format.RestoreKeyWordUppercase | format.RestoreSpacesAroundBinaryOperation,
		In:        buf,
	}
	switch v := ast.(type) {
	case *tast.CreateTableStmt:
		v.Table.Schema = model.NewCIStr("hello")
		println(v.Table.Name.L)
	case *tast.DropTableStmt:
		for i := range v.Tables {
			if v.Tables[i].Schema.String() == "b" {
				v.Tables[i].Schema = model.NewCIStr("hello")
			}
		}
	case *tast.RenameTableStmt:
		println("---")
		for i := range v.TableToTables {
			println(v.TableToTables[i].OldTable.Name.String())
			println(v.TableToTables[i].NewTable.Name.String())
		}
	case *tast.GrantStmt:
		println("is grant")
	case *tast.CreateDatabaseStmt:
		for _, opt := range v.Options {
			println(opt.Tp)
		}
	default:
		println("unknown ast")
	}

	err = ast.Restore(rCtx)
	u.PanicIfErr(err)
	fmt.Printf("after %v\n", buf.String())
}
