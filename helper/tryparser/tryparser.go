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
	sql := `CREATE TABLE t7 (
id int(11) DEFAULT NULL,
name varchar(35) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY LIST (id)
(PARTITION r0 VALUES IN (1,5,9,13,17,21) ENGINE = InnoDB,
 PARTITION r1 VALUES IN (2,6,10,14,18,22) ENGINE = InnoDB,
 PARTITION r2 VALUES IN (3,7,11,15,19,23) ENGINE = InnoDB,
 PARTITION r3 VALUES IN (4,8,12,16,20,24) ENGINE = InnoDB) */`
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
	default:
		println("unknown ast")
	}

	err = ast.Restore(rCtx)
	u.PanicIfErr(err)
	fmt.Printf("after %v\n", buf.String())
}
