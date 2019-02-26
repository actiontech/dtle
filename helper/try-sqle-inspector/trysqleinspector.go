package main

import (
	"github.com/pingcap/parser"
	"log"
	"github.com/actiontech/dtle/internal/client/driver/mysql/sqle/inspector"
)


var (
	ctx = inspector.NewContext(nil)
	p = parser.New()
)

func do(sql string) {
	ast, err := p.ParseOneStmt(sql, "", "");
	panicIfErr(err)
	ctx.UpdateContext(ast, "mysql")
}

func main() {
	ctx.LoadSchemas(nil)
	do("create schema a")
	ctx.LoadTables("a", nil)
	ctx.UseSchema("a")
	do("CREATE TABLE `a` (`id` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=46 DEFAULT CHARSET=latin1")

	log.Printf("hasTable %v", ctx.HasTable("a", "a"))

	do("alter table a.a add column value int")

	tableInfo, exist := ctx.GetTable("a", "a")
	if !exist {
		panic("shoud exist")
	}

	for _, col := range tableInfo.MergedTable.Cols {
		log.Printf("name %v tp %v", col.Name, col.Tp)

	}
}

func panicIfErr(err interface{}, args ...interface{}) {
	if err != nil {
		log.Panicf("will panic. err %v, args: %v", err, args)
	}
}
