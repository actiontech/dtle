package main

import (
	"fmt"

	"github.com/actiontech/dtle/driver/mysql/base"
	"github.com/actiontech/dtle/driver/mysql/sqle/inspector"

	//"github.com/actiontech/dtle/internal/client/driver/mysql/base"
	//"github.com/actiontech/dtle/internal/client/driver/mysql/sqle/inspector"
	"log"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
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
	//do("create schema a")
	//ctx.LoadTables("a", nil)
	//ctx.UseSchema("a")

	case8()
	//case6()
	//case5()
	//case4()
	//case1()
	//case2()
	//case3()
}

func panicIfErr(err interface{}, args ...interface{}) {
	if err != nil {
		log.Panicf("will panic. err %v, args: %v", err, args)
	}
}

func case8() { // #939
	do("create schema a")
	ctx.LoadTables("a", nil)
	ctx.UseSchema("a")

	do(`create table a.a939 (id int primary key auto_increment,
  val1 varchar(50) not null default 'aaa',
  val2 varchar(50) not null default '',
  val3 varchar(50) not null);`)

	colList, _, err := base.GetTableColumnsSqle(ctx, "a", "a939")
	panicIfErr(err)
	for _, col := range colList.Columns {
		fmt.Printf("%v default '%v'\n", col.RawName, col.Default)
	}
}

func case7() {
	do("create schema fk")
	ctx.LoadTables("fk", nil)
	ctx.UseSchema("fk")

	do("create table fk.a (id int primary key)")
	do("create table fk.a1 (id int primary key)")
//	do(`create table fk.b (id int primary key, val int,
//constraint b_ibfk_1 foreign key b_ibfk_1 (val) references fk.a (id) on update cascade,
//constraint b_ibfk_2 foreign key b_ibfk_2 (val) references fk.a1 (id) on update cascade)`)
	do(`create table fk.b (id int primary key)`)
	do(`alter table fk.b add column val int`)
	//do(`alter table fk.b add constraint foreign key (val) references fk.a (id)`)
	//do(`alter table fk.b add constraint foreign key (val) references fk.a1 (id)`)
	//do(`alter table fk.b drop foreign key b_ibfk_2`)
	ti, exist := ctx.GetTable("fk", "b")
	if !exist {
		panic("shoud exist")
	}

	if ti.MergedTable == nil {
		log.Printf("ti.MergedTable is nil")
	}
	for _, constraint := range ti.MergedTable.Constraints {
		switch constraint.Tp {
		case ast.ConstraintForeignKey:
			log.Printf("fk parent table %v", constraint.Refer.Table)
		}
	}
}
func case6() {
	do("create schema a")
	ctx.LoadTables("a", nil)
	ctx.UseSchema("a")
	do("CREATE TABLE `c` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs")
	_, exist := ctx.GetTable("a", "c")
	if !exist {
		panic("shoud exist")
	}
}
func case5() {
	log.Printf("---- case 5")
	do("create schema a")
	//schemaName := "tomasdata_new"
	schemaName := "a"
	tableName := "HistoryAlarmData"
	//tableName := "aaa"
	ctx.LoadTables("a", nil)
	ctx.UseSchema("a")

	do("CREATE TABLE `HistoryAlarmData` (`product` varchar(15) NOT NULL, `partcode` varchar(12) NOT NULL, `boardtype` varchar(20) NOT NULL, `alarmtime` datetime NOT NULL, `alarmobjectid` int(7) NOT NULL, `recoverTime` datetime NOT NULL, `alarmcode` varchar(45) NOT NULL, `area` varchar(15) NOT NULL, `city` varchar(15) NOT NULL, `subnetid` int(15) NOT NULL, `recovertype` varchar(15) NOT NULL, `duration` time NOT NULL, `sitetype` varchar(10) NOT NULL, `networkelement` varchar(45) NOT NULL, `innerlocation` varchar(127) NOT NULL, `additionaltext` varchar(255) NOT NULL, PRIMARY KEY (`alarmtime`))")

	tableInfo, exist := ctx.GetTable(schemaName, tableName)
	if !exist {
		panic("shoud exist")
	}

	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	colList, _, err := base.GetTableColumnsSqle(ctx, schemaName, tableName)
	if err != nil {
		panicIfErr(err, "at GetTableColumnsSqle")
	}
	for _, col := range colList.ColumnList() {
		log.Printf("col %v %v %v %v %v", col.RawName, col.Type, col.IsPk(), col.Nullable, col.Default)
	}

	for _, col := range cStmt.Cols {
		log.Printf("name %v tp %v", col.Name, col.Tp)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionNoOption:
			case ast.ColumnOptionPrimaryKey:
				log.Printf("  pk")
			case ast.ColumnOptionNotNull:
				log.Printf("  not null")
			case ast.ColumnOptionAutoIncrement:
				log.Printf("  auto incr")
			case ast.ColumnOptionDefaultValue:
				log.Printf("  default %v", opt.Expr.Text())
			case ast.ColumnOptionUniqKey:
				log.Printf("  unique")
			case ast.ColumnOptionNull:
				log.Printf("  null")
			case ast.ColumnOptionOnUpdate:
			case ast.ColumnOptionFulltext:
			case ast.ColumnOptionComment:
			case ast.ColumnOptionGenerated:
			case ast.ColumnOptionReference:
			}
		}
	}
}

func case4() {
	log.Printf("---- case 4")
	do("create table a.a (id int primary key default 42, val1 varchar(50))")
	do("alter table a.a rename aaa")

	tableInfo, exist := ctx.GetTable("a", "aaa")
	if !exist {
		panic("shoud exist")
	}

	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	colList, _, err := base.GetTableColumnsSqle(ctx, "a", "aaa")
	if err != nil {
		panicIfErr(err, "at GetTableColumnsSqle")
	}
	for _, col := range colList.ColumnList() {
		log.Printf("col %v %v %v %v %v", col.RawName, col.Type, col.IsPk(), col.Nullable, col.Default)
	}

	for _, col := range cStmt.Cols {
		log.Printf("name %v tp %v", col.Name, col.Tp)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionNoOption:
			case ast.ColumnOptionPrimaryKey:
				log.Printf("  pk")
			case ast.ColumnOptionNotNull:
				log.Printf("  not null")
			case ast.ColumnOptionAutoIncrement:
				log.Printf("  auto incr")
			case ast.ColumnOptionDefaultValue:
				log.Printf("  default %v", opt.Expr.Text())
			case ast.ColumnOptionUniqKey:
				log.Printf("  unique")
			case ast.ColumnOptionNull:
				log.Printf("  null")
			case ast.ColumnOptionOnUpdate:
			case ast.ColumnOptionFulltext:
			case ast.ColumnOptionComment:
			case ast.ColumnOptionGenerated:
			case ast.ColumnOptionReference:
			}
		}
	}
}

func case1() {
	log.Printf("---- case 1")
	do("create table a.a (id int primary key default 42, val1 varchar(50))")
	do("alter table a.a add column val2 int after id;")

	tableInfo, exist := ctx.GetTable("a", "a")
	if !exist {
		panic("shoud exist")
	}

	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	colList, _, err := base.GetTableColumnsSqle(ctx, "a", "a")
	if err != nil {
		panicIfErr(err, "at GetTableColumnsSqle")
	}
	for _, col := range colList.ColumnList() {
		log.Printf("col %v %v %v %v %v", col.RawName, col.Type, col.IsPk(), col.Nullable, col.Default)
	}

	for _, col := range cStmt.Cols {
		log.Printf("name %v tp %v", col.Name, col.Tp)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionNoOption:
			case ast.ColumnOptionPrimaryKey:
				log.Printf("  pk")
			case ast.ColumnOptionNotNull:
				log.Printf("  not null")
			case ast.ColumnOptionAutoIncrement:
				log.Printf("  auto incr")
			case ast.ColumnOptionDefaultValue:
				log.Printf("  default %v", opt.Expr.Text())
			case ast.ColumnOptionUniqKey:
				log.Printf("  unique")
			case ast.ColumnOptionNull:
				log.Printf("  null")
			case ast.ColumnOptionOnUpdate:
			case ast.ColumnOptionFulltext:
			case ast.ColumnOptionComment:
			case ast.ColumnOptionGenerated:
			case ast.ColumnOptionReference:
			}
		}
	}
}

func case2() {
	log.Printf("---- case 2")
	// drop table if exists a.b;
	do("create table a.b (id1 int, id2 int, val1 int, primary key (id1, id2))")

	tableInfo, exist := ctx.GetTable("a", "b")
	if !exist {
		panic("shoud exist")
	}
	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	colList, _, err := base.GetTableColumnsSqle(ctx, "a", "b")
	if err != nil {
		panicIfErr(err, "at GetTableColumnsSqle")
	}
	for _, col := range colList.ColumnList() {
		log.Printf("col %v %v %v %v", col.RawName, col.Type, col.IsPk(), col.Nullable)
	}

}

func case3() {
	log.Printf("---- case 3")
	// drop table if exists a.b;
	//do("create table a.c (id int primary key, val1 decimal(10,2))")
	do("create table a.c (id int primary key, val1 datetime(6))")

	tableInfo, exist := ctx.GetTable("a", "c")
	if !exist {
		panic("shoud exist")
	}
	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	colList, _, err := base.GetTableColumnsSqle(ctx, "a", "c")
	if err != nil {
		panicIfErr(err, "at GetTableColumnsSqle")
	}
	for _, col := range colList.ColumnList() {
		log.Printf("col %v %v %v %v", col.RawName, col.Type, col.IsPk(), col.Nullable)
	}
}
