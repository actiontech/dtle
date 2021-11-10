package main

import (
	"bytes"
	"github.com/actiontech/dtle/helper/u"
	tparser "github.com/pingcap/parser"
	tast "github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"

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
		JoinLevel: 0,
	}
	switch v := ast.(type) {
	case *tast.CreateTableStmt:
		v.Table.Schema.O = "hello"
	case *tast.DropTableStmt:
		for i := range v.Tables {
			if v.Tables[i].Schema.O == "b" {
				v.Tables[i].Schema.O = "hello"
			}
		}
	case *tast.RenameTableStmt:
		println(v.OldTable.Name.O)
		println(v.NewTable.Name.O)
		println("---")
		for i := range v.TableToTables {
			println(v.TableToTables[i].OldTable.Name.O)
			println(v.TableToTables[i].NewTable.Name.O)
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
