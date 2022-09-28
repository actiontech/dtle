package main

import (
	"bytes"
	"github.com/actiontech/dtle/helper/u"
	tparser "github.com/pingcap/tidb/parser"
	tast "github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	//"golang.org/x/text/encoding/simplifiedchinese"
	//"golang.org/x/text/transform"
	//"strings"

	"fmt"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"log"
)

func main() {
	fmt.Printf("")
	log.Println("hello")
	var err error

	p := tparser.New()
	//sql := "create table b.t1 (id int primary key auto_increment, val timestamp not null default '0000-00-00 00:00:00')"
	//sql := "create table b.t1 (id int primary key auto_increment, val timestamp not null default 0)"
	//sql := "create table b.t1 (id int primary key auto_increment, val varchar(64) not null default 'hello')"

	//sb := strings.Builder{}
	//gbkStr, _, err := transform.String(simplifiedchinese.GBK.NewEncoder(),
	//	"create table s中文.t中文 (id中文 int primary key auto_increment,\n ")
	//u.PanicIfErr(err)
	//sb.WriteString(gbkStr)
	//sb.WriteString("val varchar(50) default _utf8mb4\"aa中文\")")
	//sql := sb.String()

	//sql := "drop view a.v1"
	//sql := "alter view a.v1 as select * from a.a"
	sql := "CREATE DATABASE a DEFAULT CHARACTER SET gb2312"
	ast, err := p.ParseOneStmt(sql, "", "")

	u.PanicIfErr(err)

	buf := bytes.NewBuffer(nil)
	rCtx := &format.RestoreCtx{
		//Flags:     format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes | format.RestoreSpacesAroundBinaryOperation,
		//Flags:     format.RestoreKeyWordUppercase | format.RestoreSpacesAroundBinaryOperation,
		Flags:     format.DefaultRestoreFlags | format.RestoreStringWithoutDefaultCharset,
		In:        buf,
	}
	switch v := ast.(type) {
	case *tast.CreateTableStmt:
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
