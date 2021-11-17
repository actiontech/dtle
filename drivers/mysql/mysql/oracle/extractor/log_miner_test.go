package extractor

import (
	"testing"

	"github.com/hashicorp/go-hclog"
)

func TestParseDDLSQL(t *testing.T) {

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "createTableSQLCharRelation",
			sql: `CREATE TABLE test."persons"(
				   "first_name" VARCHAR(15) NOT NULL,
				   last_name VARCHAR2(45) NOT NULL
				 );`,
			want: "CREATE TABLE TEST.persons (first_name VARCHAR(15),LAST_NAME VARCHAR(45))"},
		{
			// 是否支持没有 p s,或者仅仅支持 p s
			//NUMERIC_NAME  NUMERIC(15,2),
			//Decimal_NAME  DECIMAL(15,2),
			//Dec_NAME DEC(15,2),
			// ps 都为空时候，oracle上限为 38,0 mysql上限为10,0
			name: "createTableSQLNumberRelation",
			sql: `CREATE TABLE test."persons"(	
    			   "first_num" NUMBER(15,2) NOT NULL,
    			    second_num NUMBER(10) NOT NULL,
					three_num NUMBER(5,0) NOT NULL,
					last_name NUMBER NOT NULL,
					NUMERIC_NAME  NUMERIC(15,2),
					Decimal_NAME  DECIMAL(15,2),
					Dec_NAME DEC(15,2),
					INTEGER_NAME INTEGER,
					INT_NAME  INT,
					SMALLINT_NAME SMALLINT
				 );`,
			want: "CREATE TABLE TEST.persons (first_num DECIMAL(15,2),SECOND_NUM BIGINT,THREE_NUM DECIMAL(5,0),LAST_NAME DOUBLE,NUMERIC_NAME NUMERIC(15,2) ,DECIMAL_NAME DECIMAL(15,2) ,DEC_NAME DEC(15,2) ,INTEGER_NAME INT,INT_NAME INT,SMALLINT_NAME DECIMAL(38))"},
	}
	logger := hclog.NewNullLogger()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := parseDDLSQL(logger, tt.sql)
			if err != nil {
				t.Error(err)
				return
			}
			if dataEvent.Query != tt.want {
				t.Errorf("parseDDLSQL() = %v, want %v", dataEvent.Query, tt.want)
			}
		})
	}
}

func TestParseDDLSQLAlter(t *testing.T) {
	logger := hclog.NewNullLogger()
	testAlter := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "alterTableSQLAdd",
			sql: `ALTER TABLE test."persons" ADD (
					add_name1 VARCHAR2 ( 30 )) ADD (
					add_name2 VARCHAR2 ( 30 ));`,
			want: "ALTER TABLE TEST.persons ADD COLUMN(ADD_NAME1 VARCHAR(30)),ADD COLUMN(ADD_NAME2 VARCHAR(30))"},
		{
			name: "alterTableSQLModify",
			sql: `ALTER TABLE test."persons" MODIFY (
					alter_new_name1 CHAR ( 13 )) MODIFY (
					alter_name2 VARCHAR ( 66 ))`,
			want: "ALTER TABLE TEST.persons MODIFY COLUMN(ALTER_NEW_NAME1 CHAR(13)),MODIFY COLUMN(ALTER_NAME2 VARCHAR(66))"},
		{
			name: "alterTableSQLDrop",
			sql:  `ALTER TABLE "TEST"."persons" DROP ("DROP_NAME1",drop_name2)`,
			want: "ALTER TABLE TEST.persons DROP COLUMN(DROP_NAME1,DROP_NAME2)"},
		{
			name: "alterTableSQLRename",
			sql:  `ALTER TABLE "TEST"."persons" RENAME COLUMN "RE_NAME" TO "RE_NAME_NEW"`,
			want: "ALTER TABLE TEST.persons RENAME COLUMN RE_NAME TO RE_NAME_NEW"},
		// index
	}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := parseDDLSQL(logger, tt.sql)
			if err != nil {
				t.Error(err)
				return
			}
			if dataEvent.Query != tt.want {
				t.Errorf("parseDDLSQL() = %v, want %v", dataEvent.Query, tt.want)
			}
		})
	}
}

func TestParseDDLSQLDROP(t *testing.T) {
	logger := hclog.NewNullLogger()
	testAlter := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "DropTableSQL",
			sql:  `DROP TABLE test."persons";`,
			want: "DROP TABLE TEST.persons"},
		// index
	}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := parseDDLSQL(logger, tt.sql)
			if err != nil {
				t.Error(err)
				return
			}
			if dataEvent.Query != tt.want {
				t.Errorf("parseDDLSQL() = %v, want %v", dataEvent.Query, tt.want)
			}
		})
	}
}
