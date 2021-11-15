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
			name: "createTableSQLNumberRelation",
			sql: `CREATE TABLE test."persons"(	
    			   "first_num" NUMBER(15,2) NOT NULL,
    			    second_num NUMBER(10) NOT NULL,
					three_num NUMBER(5,0) NOT NULL,
					last_name NUMBER NOT NULL,
					INT_NAME  INT,
					SMALLINT_NAME SMALLINT
				 );`,
			want: "CREATE TABLE TEST.persons (first_name DECIMAL(15,2),SECOND_NAME BIGINT,THREE_NAME DECIMAL(5,0),LAST_NAME DOUBLE)"},
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

// ps 都有 sql解析失败， 少了任意一个都会导致空指针，新的数据结构需要p s （可以用0替代？）
//NUMERIC_NAME  NUMERIC(15,2),
//Decimal_NAME  DECIMAL(15,2),
//Dec_NAME DEC(15,2),
// 解析不了
// INTEGER
