package extractor

import (
	"testing"

	"github.com/actiontech/dtle/drivers/driver/common"

	"github.com/hashicorp/go-hclog"
)

func TestBuildFilterSchemaTable(t *testing.T) {
	replicateDoDb0 := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db1",
				},
				{
					TableName:   "tb2",
					TableSchema: "db1",
				},
				{
					TableName:   "tb3",
					TableSchema: "db1",
				},
				{
					TableName: "tb-skip",
				},
			},
		},
	}

	replicateIgnoreDB0 := []*common.DataSource{}
	replicateDoDb1 := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db1",
				},
				{
					TableName:   "tb2",
					TableSchema: "db1",
				},
				{
					TableName:   "tb3",
					TableSchema: "db1",
				},
				{
					TableName: "tb-skip",
				},
			},
		},
		{
			TableSchema: "db2",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db2",
				},
			},
		},
		{
			TableSchema: "db3",
			Tables: []*common.Table{
				{
					TableName:   "tb1",
					TableSchema: "db3",
				},
			},
		},
	}

	replicateIgnoreDB1 := []*common.DataSource{
		{
			TableSchema: "db1",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
		{
			TableSchema: "db2",
			Tables: []*common.Table{
				{
					TableName: "tb-skip",
				},
			},
		},
		{
			TableSchema: "db3",
		},
		{
			TableSchema: "db4",
			Tables: []*common.Table{
				{
					TableName: "tb1",
				},
			},
		},
	}

	tests := []struct {
		name              string
		replicateDoDb     []*common.DataSource
		replicateIgnoreDB []*common.DataSource
		want              string
	}{
		{
			name:              "replicateDoDb0",
			replicateDoDb:     replicateDoDb0,
			replicateIgnoreDB: replicateIgnoreDB0,
			want:              " AND( ( seg_owner = 'db1' AND table_name in ('tb1','tb2','tb3','tb-skip')))"},
		{
			name:              "replicateDoDb1",
			replicateDoDb:     replicateDoDb1,
			replicateIgnoreDB: replicateIgnoreDB1,
			want:              " AND( ( seg_owner = 'db1' AND table_name in ('tb1','tb2','tb3','tb-skip')) OR ( seg_owner = 'db2' AND table_name in ('tb1')) OR ( seg_owner = 'db3' AND table_name in ('tb1'))) AND ( seg_owner = 'db1' AND table_name not in ('tb1')) AND ( seg_owner = 'db2' AND table_name not in ('tb-skip')) AND ( seg_owner <> 'db3') AND ( seg_owner = 'db4' AND table_name not in ('tb1'))"},
		{
			name:              "empty",
			replicateDoDb:     []*common.DataSource{},
			replicateIgnoreDB: []*common.DataSource{},
			want:              ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logminer := NewLogMinerStream(nil, hclog.NewNullLogger(), tt.replicateDoDb, tt.replicateIgnoreDB, 0, 0, 100000)
			filterSQL := logminer.buildFilterSchemaTable()
			if filterSQL != tt.want {
				t.Errorf("parseDDLSQL() = %v, want %v", filterSQL, tt.want)
			}
		})
	}
}
func TestParseDMLSQL(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		undo_sql  string
		want_rows [][]interface{}
	}{
		{
			name:      "NCHAR_255_COLUMNS",
			sql:       `insert into "TEST"."NCHAR_255_COLUMNS"("COL1","COL2") values ('11',UNISTR('\6570\636E\5E93sql\6D4B\8BD5\6570\636E\5E93sql\6D4B\8BD5                                                                                                                                                                                                                                               '))`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"11", "数据库sql测试数据库sql测试                                                                                                                                                                                                                                               "}}},
		// NUMBER(*)
		// BFILE  no support
		// BINARY_FLOAT
		// insert into TEST.TEST("COL1","COL2") values (1, 3.40282E+38F);
		// insert into TEST.TEST("COL1","COL2") values (2, BINARY_FLOAT_INFINITY);
		// insert into TEST.TEST("COL1","COL2") values (3, -BINARY_FLOAT_INFINITY);
		// insert into TEST.TEST("COL1","COL2") values (4, BINARY_FLOAT_NAN);
		{
			name:      "BINARY_FLOAT1",
			sql:       `insert into "TEST"."BINARY_FLOAT1"("COL1","COL2") values ('0', '1.17549E-38F');`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"0", "1.17549E-38F"}},
		},
		{
			name:      "BINARY_FLOAT2",
			sql:       `insert into TEST.BINARY_FLOAT2("COL1","COL2") values ('1', '3.40282E+38F');`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"1", "3.40282E+38F"}},
		},
		{
			name:      "BINARY_FLOAT3",
			sql:       `insert into "TEST"."BINARY_FLOAT3"("COL1","COL2") values ('2', 'Inf');`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"2", nil}},
		},
		{
			name:      "BINARY_FLOAT4",
			sql:       `insert into "TEST"."BINARY_FLOAT4"("COL1","COL2") values ('3', '-Inf');`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"3", nil}},
		},
		{
			name:      "BINARY_FLOAT5",
			sql:       `insert into "TEST"."BINARY_FLOAT5"("COL1","COL2") values ('4', 'Nan');`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"4", nil}},
		},
		{
			name:      "BINARY_FLOAT6",
			sql:       `update "TEST"."BINARY_FLOAT6" set "COL2" ='500'  where "COL1" = '3' and "COL2" = 'NULL';`,
			undo_sql:  `update "TEST"."BINARY_FLOAT6" set "COL2" = NULL  where "COL1" = '3' and "COL2" = '50\0';`,
			want_rows: [][]interface{}{{"3", nil}, {"3", "50\\0"}},
		},
		{
			name:      "BINARY_FLOAT7",
			sql:       `delete from "TEST"."BINARY_FLOAT7" where "COL1" = '4' and "COL2" = 'Nan';`,
			undo_sql:  `insert into "TEST"."BINARY_FLOAT7"("COL1","COL2") VALUES ('4', 'Nan');`,
			want_rows: [][]interface{}{{"4", nil}},
		},
		// insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('1',NULL)
		// insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('2',TO_DATE('-4712-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'))
		// insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('3',TO_DATE(' 9999-12-31 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'))
		// insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('4',TO_DATE(' 2003-05-03 21:02:44', 'SYYYY-MM-DD HH24:MI:SS'))
		{
			name:      "DATE_COLUMNS",
			sql:       `insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('1',NULL)`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"1", nil}},
		},
		{
			name:      "DATE_COLUMNS",
			sql:       `insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('2',TO_DATE('-4712-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'))`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"2", "-4712-01-01 00:00:00"}},
		},
		{
			name:      "DATE_COLUMNS",
			sql:       `insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('3',TO_DATE(' 9999-12-31 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'))`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"3", " 9999-12-31 00:00:00"}},
		},
		{
			name:      "DATE_COLUMNS",
			sql:       `insert into "TEST"."DATE_COLUMNS"("COL1","COL2") values ('4',TO_DATE(' 2003-05-03 21:02:44', 'SYYYY-MM-DD HH24:MI:SS'))`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"4", " 2003-05-03 21:02:44"}},
		},
		// CREATE TABLE TEST."te\shu"(COL1 INT, col2 CHAR(256))
		{
			name:      `te\shu`,
			sql:       `insert into "TEST"."te\shu"("COL1","COL2") values ('5','x\x44')`,
			undo_sql:  `insert into "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('5', 'Nan');`,
			want_rows: [][]interface{}{{"5", `x\x44`}},
		},
		{
			name:      `te\shu`,
			sql:       `delete from "TEST"."te\shu"  where "COL1" = '4' and "COL2" = '\';`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"4", `\`}},
		},
		{
			name:      `te\shu`,
			sql:       `delete from "TEST"."te\shu" where "COL1" = '5' and "COL2" = '"';`,
			undo_sql:  `insert into "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('5', 'Nan');`,
			want_rows: [][]interface{}{{"5", `"`}},
		},
		// {
		// 	name:      "NCHAR_255_COLUMNS",
		// 	sql:       `insert into "TEST"."NCHAR_255_COLUMNS"("COL1","COL2") values ('9',UNISTR('\6570\636E\5E93sql\6D4B\8BD5'))`,
		// 	undo_sql:  ``,
		// 	want_rows: [][]interface{}{{"9", "数据库sql测试"}}},
		{
			name:      "CHAR_255_COLUMNS2",
			sql:       `insert into "TEST"."CHAR_255_COLUMNS2"("COL1","COL2") values ('16','"')`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"16", `"`}}},
		{
			name:      "CHAR_255_COLUMNS3",
			sql:       `insert into "TEST"."CHAR_255_COLUMNS3"("COL1","COL2") values ('18','\')`,
			undo_sql:  ``,
			want_rows: [][]interface{}{{"18", `\`}}},
	}

	logger := hclog.NewNullLogger()
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// ==== load test config start
			schemaConfig := extractor.findSchemaConfig("TEST")
			tableConfig := findTableConfig(schemaConfig, tt.name)
			tableConfig.OriginalTableColumns = &common.ColumnList{
				Ordinals: map[string]int{},
			}
			for i, column := range []string{"COL1", "COL2"} {
				tableConfig.OriginalTableColumns.Ordinals[column] = i
			}
			// ==== load test config end

			dataEvent, err := extractor.parseDMLSQL(tt.sql, tt.undo_sql)
			if err != nil {
				t.Error(err)
				return
			}

			if len(dataEvent.Rows) != len(tt.want_rows) {
				t.Errorf("unexpected rows")
			}
			for i := range dataEvent.Rows {
				if len(dataEvent.Rows[i]) != len(tt.want_rows[i]) {
					t.Errorf("unexpected rows")
				}
				for j := range dataEvent.Rows[i] {
					if dataEvent.Rows[i][j] != tt.want_rows[i][j] {
						t.Errorf("unexpected rows real %v ,want %v ", dataEvent.Rows[i][j], tt.want_rows[i][j])
					}
				}
			}
		})
	}
}
func TestParseDDLSQL(t *testing.T) {

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_WILDCARD_COLUMNS(ID INT, C_NUMBER NUMBER(*));`,
			want: "CREATE TABLE `TEST`.`NUMBER_WILDCARD_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BFILE_COLUMNS(ID INT, C_BFILE BFILE);`,
			want: "CREATE TABLE `TEST`.`BFILE_COLUMNS` (`ID` INT,`C_BFILE` VARCHAR(255)) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BINARY_FLOAT_COLUMNS(ID INT, C_BINARY_FLOAT BINARY_FLOAT);`,
			want: "CREATE TABLE `TEST`.`BINARY_FLOAT_COLUMNS` (`ID` INT,`C_BINARY_FLOAT` FLOAT) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BINARY_DOUBLE_COLUMNS(ID INT, C_BINARY_DOUBLE BINARY_DOUBLE);`,
			want: "CREATE TABLE `TEST`.`BINARY_DOUBLE_COLUMNS` (`ID` INT,`C_BINARY_DOUBLE` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BLOB_COLUMNS(ID INT, C_BLOB BLOB);`,
			want: "CREATE TABLE `TEST`.`BLOB_COLUMNS` (`ID` INT,`C_BLOB` LONGBLOB) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHAR_255_COLUMNS(ID INT, C_CHAR CHAR(255));`,
			want: "CREATE TABLE `TEST`.`CHAR_255_COLUMNS` (`ID` INT,`C_CHAR` CHAR(255)) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHARACTER_255_COLUMNS(ID INT, C_CHARACTER CHARACTER(255));`,
			want: "CREATE TABLE `TEST`.`CHARACTER_255_COLUMNS` (`ID` INT,`C_CHARACTER` CHAR(255)) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHARACTER_256_COLUMNS(ID INT, C_CHARACTER CHARACTER(256));`,
			want: "CREATE TABLE `TEST`.`CHARACTER_256_COLUMNS` (`ID` INT,`C_CHARACTER` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CLOB_COLUMNS(ID INT, C_CLOB CLOB);`,
			want: "CREATE TABLE `TEST`.`CLOB_COLUMNS` (`ID` INT,`C_CLOB` LONGTEXT) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DATE_COLUMNS(ID INT, C_DATE DATE);`,
			want: "CREATE TABLE `TEST`.`DATE_COLUMNS` (`ID` INT,`C_DATE` DATETIME) DEFAULT CHARACTER SET = UTF8MB4"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DECIMAL_COLUMNS(ID INT, C_DECIMAL DECIMAL(11, 3));`,
			want: "CREATE TABLE `TEST`.`DECIMAL_COLUMNS` (`ID` INT,`C_DECIMAL` DECIMAL(11,3)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.DEC_COLUMNS(ID INT, C_DEC DEC(11, 3));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DEC_COLUMNS(ID INT, C_DEC DEC(11, 3));`,
			want: "CREATE TABLE `TEST`.`DEC_COLUMNS` (`ID` INT,`C_DEC` DECIMAL(11,3)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.DOUBLE_PRECISION_COLUMNS(ID INT, C_DOUBLE_PRECISION DOUBLE PRECISION);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DOUBLE_PRECISION_COLUMNS(ID INT, C_DOUBLE_PRECISION DOUBLE PRECISION);`,
			want: "CREATE TABLE `TEST`.`DOUBLE_PRECISION_COLUMNS` (`ID` INT,`C_DOUBLE_PRECISION` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.FLOAT_COLUMNS(ID INT, C_FLOAT FLOAT(11));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.FLOAT_COLUMNS(ID INT, C_FLOAT FLOAT(11));`,
			want: "CREATE TABLE `TEST`.`FLOAT_COLUMNS` (`ID` INT,`C_FLOAT` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.INTEGER_COLUMNS(ID INT, C_INTEGER INTEGER);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTEGER_COLUMNS(ID INT, C_INTEGER INTEGER);`,
			want: "CREATE TABLE `TEST`.`INTEGER_COLUMNS` (`ID` INT,`C_INTEGER` INT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.INT_COLUMNS(ID INT, C_INT INT);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INT_COLUMNS(ID INT, C_INT INT);`,
			want: "CREATE TABLE `TEST`.`INT_COLUMNS` (`ID` INT,`C_INT` INT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.INTERVAL_YEAR_COLUMNS(ID INT, C_INTERVAL_YEAR INTERVAL YEAR(3) TO MONTH);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTERVAL_YEAR_COLUMNS(ID INT, C_INTERVAL_YEAR INTERVAL YEAR(3) TO MONTH);`,
			want: "CREATE TABLE `TEST`.`INTERVAL_YEAR_COLUMNS` (`ID` INT,`C_INTERVAL_YEAR` VARCHAR(30)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.INTERVAL_DAY_COLUMNS(ID INT, C_INTERVAL_DAY INTERVAL DAY(3) TO SECOND(5));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTERVAL_DAY_COLUMNS(ID INT, C_INTERVAL_DAY INTERVAL DAY(3) TO SECOND(5));`,
			want: "CREATE TABLE `TEST`.`INTERVAL_DAY_COLUMNS` (`ID` INT,`C_INTERVAL_DAY` VARCHAR(30)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.LONG_COLUMNS(ID INT, C_LONG LONG);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.LONG_COLUMNS(ID INT, C_LONG LONG);`,
			want: "CREATE TABLE `TEST`.`LONG_COLUMNS` (`ID` INT,`C_LONG` LONGTEXT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.LONG_RAW_COLUMNS(ID INT, C_LONG_RAW LONG RAW);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.LONG_RAW_COLUMNS(ID INT, C_LONG_RAW LONG RAW);`,
			want: "CREATE TABLE `TEST`.`LONG_RAW_COLUMNS` (`ID` INT,`C_LONG_RAW` LONGBLOB) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NCHAR_255_COLUMNS(ID INT, C_NCHAR NCHAR(255));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_255_COLUMNS(ID INT, C_NCHAR NCHAR(255));`,
			want: "CREATE TABLE `TEST`.`NCHAR_255_COLUMNS` (`ID` INT,`C_NCHAR` CHAR(255)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NCHAR_256_COLUMNS(ID INT, C_NCHAR NCHAR(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_256_COLUMNS(ID INT, C_NCHAR NCHAR(256));`,
			want: "CREATE TABLE `TEST`.`NCHAR_256_COLUMNS` (`ID` INT,`C_NCHAR` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NCHAR_VARYING_COLUMNS(ID INT, C_NCHAR_VARYING NCHAR VARYING(2000));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_VARYING_COLUMNS(ID INT, C_NCHAR_VARYING NCHAR VARYING(2000));`,
			want: "CREATE TABLE `TEST`.`NCHAR_VARYING_COLUMNS` (`ID` INT,`C_NCHAR_VARYING` VARCHAR(2000)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NCLOB_COLUMNS(ID INT, C_NCLOB NCLOB);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCLOB_COLUMNS(ID INT, C_NCLOB NCLOB);`,
			want: "CREATE TABLE `TEST`.`NCLOB_COLUMNS` (`ID` INT,`C_NCLOB` TEXT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_2_COLUMNS(ID INT, C_NUMBER NUMBER(2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_2_COLUMNS(ID INT, C_NUMBER NUMBER(2));`,
			want: "CREATE TABLE `TEST`.`NUMBER_2_COLUMNS` (`ID` INT,`C_NUMBER` TINYINT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_4_COLUMNS(ID INT, C_NUMBER NUMBER(4, 0));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_4_COLUMNS(ID INT, C_NUMBER NUMBER(4, 0));`,
			want: "CREATE TABLE `TEST`.`NUMBER_4_COLUMNS` (`ID` INT,`C_NUMBER` SMALLINT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_8_COLUMNS(ID INT, C_NUMBER NUMBER(8));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_8_COLUMNS(ID INT, C_NUMBER NUMBER(8));`,
			want: "CREATE TABLE `TEST`.`NUMBER_8_COLUMNS` (`ID` INT,`C_NUMBER` INT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_18_COLUMNS(ID INT, C_NUMBER NUMBER(18, 0));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_18_COLUMNS(ID INT, C_NUMBER NUMBER(18, 0));`,
			want: "CREATE TABLE `TEST`.`NUMBER_18_COLUMNS` (`ID` INT,`C_NUMBER` BIGINT) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38));`,
			want: "CREATE TABLE `TEST`.`NUMBER_38_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(38)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38,31));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38,31));`,
			want: "CREATE TABLE `TEST`.`NUMBER_38_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(38,30)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_8_2_COLUMNS(ID INT, C_NUMBER NUMBER(8, 2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_8_2_COLUMNS(ID INT, C_NUMBER NUMBER(8, 2));`,
			want: "CREATE TABLE `TEST`.`NUMBER_8_2_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(8,2)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_COLUMNS(ID INT, C_NUMBER NUMBER);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_COLUMNS(ID INT, C_NUMBER NUMBER);`,
			want: "CREATE TABLE `TEST`.`NUMBER_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMBER_WILDCARD_COLUMNS(ID INT, C_NUMBER NUMBER(*));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_WILDCARD_COLUMNS(ID INT, C_NUMBER NUMBER(*));`,
			want: "CREATE TABLE `TEST`.`NUMBER_WILDCARD_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NUMERIC_COLUMNS(ID INT, C_NUMERIC NUMERIC(8, 2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMERIC_COLUMNS(ID INT, C_NUMERIC NUMERIC(8, 2));`,
			want: "CREATE TABLE `TEST`.`NUMERIC_COLUMNS` (`ID` INT,`C_NUMERIC` DECIMAL(8,2)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.NVARCHAR2_COLUMNS(ID INT, C_NVARCHAR2 NVARCHAR2(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NVARCHAR2_COLUMNS(ID INT, C_NVARCHAR2 NVARCHAR2(256));`,
			want: "CREATE TABLE `TEST`.`NVARCHAR2_COLUMNS` (`ID` INT,`C_NVARCHAR2` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.RAW_COLUMNS(ID INT, C_RAW RAW(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.RAW_COLUMNS(ID INT, C_RAW RAW(256));`,
			want: "CREATE TABLE `TEST`.`RAW_COLUMNS` (`ID` INT,`C_RAW` VARBINARY(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.REAL_COLUMNS(ID INT, C_REAL REAL);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.REAL_COLUMNS(ID INT, C_REAL REAL);`,
			want: "CREATE TABLE `TEST`.`REAL_COLUMNS` (`ID` INT,`C_REAL` DOUBLE) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.ROWID_COLUMNS(ID INT, C_ROWID ROWID);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.ROWID_COLUMNS(ID INT, C_ROWID ROWID);`,
			want: "CREATE TABLE `TEST`.`ROWID_COLUMNS` (`ID` INT,`C_ROWID` CHAR(100)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.SMALLINT_COLUMNS(ID INT, C_SMALLINT SMALLINT);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.SMALLINT_COLUMNS(ID INT, C_SMALLINT SMALLINT);`,
			want: "CREATE TABLE `TEST`.`SMALLINT_COLUMNS` (`ID` INT,`C_SMALLINT` DECIMAL(38)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.TIMESTAMP_COLUMNS(ID INT, C_TIMESTAMP TIMESTAMP(9));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.TIMESTAMP_COLUMNS(ID INT, C_TIMESTAMP TIMESTAMP(9));`,
			want: "CREATE TABLE `TEST`.`TIMESTAMP_COLUMNS` (`ID` INT,`C_TIMESTAMP` DATETIME(6)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.TIMESTAMP_ZONE_COLUMNS(ID INT, C_TIMESTAMP_ZONE TIMESTAMP(9) WITH TIME ZONE);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.TIMESTAMP_ZONE_COLUMNS(ID INT, C_TIMESTAMP_ZONE TIMESTAMP(9) WITH TIME ZONE);`,
			want: "CREATE TABLE `TEST`.`TIMESTAMP_ZONE_COLUMNS` (`ID` INT,`C_TIMESTAMP_ZONE` DATETIME(6)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.UROWID_COLUMNS(ID INT, C_UROWID UROWID(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.UROWID_COLUMNS(ID INT, C_UROWID UROWID(256));`,
			want: "CREATE TABLE `TEST`.`UROWID_COLUMNS` (`ID` INT,`C_UROWID` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.VARCHAR_COLUMNS(ID INT, C_VARCHAR VARCHAR(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.VARCHAR_COLUMNS(ID INT, C_VARCHAR VARCHAR(256));`,
			want: "CREATE TABLE `TEST`.`VARCHAR_COLUMNS` (`ID` INT,`C_VARCHAR` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		//CREATE TABLE TEST.VARCHAR2_COLUMNS(ID INT, C_VARCHAR2 VARCHAR2(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.VARCHAR2_COLUMNS(ID INT, C_VARCHAR2 VARCHAR2(256));`,
			want: "CREATE TABLE `TEST`.`VARCHAR2_COLUMNS` (`ID` INT,`C_VARCHAR2` VARCHAR(256)) DEFAULT CHARACTER SET = UTF8MB4"},
		// CREATE TABLE TEST.XMLTYPE_COLUMNS(ID INT, C_XMLTYPE XMLTYPE);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.XMLTYPE_COLUMNS(ID INT, C_XMLTYPE XMLTYPE);`,
			want: "CREATE TABLE `TEST`.`XMLTYPE_COLUMNS` (`ID` INT,`C_XMLTYPE` LONGTEXT) DEFAULT CHARACTER SET = UTF8MB4"},

		// {
		// 	name: "createTableSQLCharRelation",
		// 	sql: `CREATE TABLE test."persons"(
		// 		   "first_name" VARCHAR(15) NOT NULL,
		// 		   last_name VARCHAR2(45) NOT NULL
		// 		 );`,
		// 	want: "CREATE TABLE `TEST`.`persons (first_name VARCHAR(15),LAST_NAME VARCHAR(45)) DEFAULT CHARACTER SET = UTF8MB4"},
		// {
		// 	// 是否支持没有 p s,或者仅仅支持 p s
		// 	//NUMERIC_NAME  NUMERIC(15,2),
		// 	//Decimal_NAME  DECIMAL(15,2),
		// 	//Dec_NAME DEC(15,2),
		// 	// ps 都为空时候，oracle上限为 38,0 mysql上限为10,0
		// 	name: "createTableSQLNumberRelation",
		// 	sql: `CREATE TABLE test."persons"(
		// 		   "first_num" NUMBER(15,2) NOT NULL,
		// 		    second_num NUMBER(10) NOT NULL,
		// 			three_num NUMBER(5,0) NOT NULL,
		// 			last_name NUMBER NOT NULL,
		// 			NUMERIC_NAME  NUMERIC(15,2),
		// 			Decimal_NAME  DECIMAL(15,2),
		// 			Dec_NAME DEC(15,2),
		// 			INTEGER_NAME INTEGER,
		// 			INT_NAME  INT,
		// 			SMALLINT_NAME SMALLINT
		// 		 );`,
		// 	want: "CREATE TABLE `TEST`.`persons (first_num DECIMAL(15,2),SECOND_NUM BIGINT,THREE_NUM INT,LAST_NAME DOUBLE,NUMERIC_NAME NUMERIC(15,2),DECIMAL_NAME DECIMAL(15,2),DEC_NAME DEC(15,2),INTEGER_NAME INT,INT_NAME INT,SMALLINT_NAME DECIMAL(38)) DEFAULT CHARACTER SET = UTF8MB4"},
	}
	logger := hclog.NewNullLogger()
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := extractor.parseDDLSQL(tt.sql, "")
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
			want: "DROP TABLE `TEST`.`persons`"},
		// index
	}
	extractor := &ExtractorOracle{logger: logger}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := extractor.parseDDLSQL(tt.sql, "")
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

func TestParseAlterTable(t *testing.T) {
	logger := hclog.NewNullLogger()
	testAlter := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "ADDCOLUMN",
			sql:  `alter table TEST.ADDCOLUMN add (author_last_published date);`,
			want: "ALTER TABLE `TEST`.`ADDCOLUMN` ADD COLUMN (`AUTHOR_LAST_PUBLISHED` DATETIME)"},
		{
			name: "MODIFYCOLUMN",
			sql:  `ALTER TABLE test."MODIFYCOLUMN" MODIFY ( alter_new_name1 CHAR ( 13 )) MODIFY ( alter_name2 VARCHAR ( 66 ))`,
			want: "ALTER TABLE `TEST`.`MODIFYCOLUMN` MODIFY COLUMN `ALTER_NEW_NAME1` CHAR(13), MODIFY COLUMN `ALTER_NAME2` VARCHAR(66)"},
		{
			name: "DROPCOLUMN",
			// alter table table_name drop (column_name1, column_name2);
			sql:  `alter table TEST.DROPCOLUMN drop (COL1, COL2);`,
			want: "ALTER TABLE `TEST`.`DROPCOLUMN` DROP COLUMN `TEST`.`DROPCOLUMN`.`COL1`, DROP COLUMN `TEST`.`DROPCOLUMN`.`COL2`"},
		{
			name: "DROPCOLUMN1",
			// alter table table_name drop column column_name;
			sql:  `alter table TEST.DROPCOLUMN1 drop column COL1`,
			want: "ALTER TABLE `TEST`.`DROPCOLUMN1` DROP COLUMN `TEST`.`DROPCOLUMN1`.`COL1`"},
		{
			name: "RENAMECOLUMN",
			// alter table table_name drop column column_name;
			sql:  `alter table TEST.RENAMECOLUMN RENAME  COLUMN COL1 TO COLNEW1`,
			want: "ALTER TABLE `TEST`.`RENAMECOLUMN` RENAME COLUMN `TEST`.`RENAMECOLUMN`.`COL1` TO `TEST`.`RENAMECOLUMN`.`COLNEW1`"},
	}
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			// ==== load test config start
			schemaConfig := extractor.findSchemaConfig("TEST")
			tableConfig := findTableConfig(schemaConfig, tt.name)
			tableConfig.OriginalTableColumns = &common.ColumnList{
				Ordinals: map[string]int{},
			}
			for i, column := range []string{"COL1", "COL2"} {
				tableConfig.OriginalTableColumns.Ordinals[column] = i
			}
			// ==== load test config end

			dataEvent, err := extractor.parseDDLSQL(tt.sql, "")
			if err != nil {
				t.Error(err)
				return
			}
			if dataEvent.Query != tt.want {
				t.Errorf("alterTableSQL() = %v, want %v", dataEvent.Query, tt.want)
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	logger := hclog.NewNullLogger()
	testAlter := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "DROPTABLE",
			sql:  `DROP TABLE TEST.DROPTABLE`,
			want: "DROP TABLE `TEST`.`DROPTABLE`"},
	}
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			// ==== load test config start
			schemaConfig := extractor.findSchemaConfig("TEST")
			tableConfig := findTableConfig(schemaConfig, tt.name)
			tableConfig.OriginalTableColumns = &common.ColumnList{
				Ordinals: map[string]int{},
			}
			for i, column := range []string{"COL1", "COL2"} {
				tableConfig.OriginalTableColumns.Ordinals[column] = i
			}
			// ==== load test config end

			dataEvent, err := extractor.parseDDLSQL(tt.sql, "")
			if err != nil {
				t.Error(err)
				return
			}
			if dataEvent.Query != tt.want {
				t.Errorf("alterTableSQL() = %v, want %v", dataEvent.Query, tt.want)
			}
		})
	}
}

func TestParseConstraintSQL(t *testing.T) {

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "createTableSQLCharRelation",
			sql: `create table TEST.userInfo (  
				id number(6) primary key,--主键  
				name varchar2(20) not null,--非空  
				sex number(1),  
				age number(3) default 18,  
				birthday date,  
				address varchar2(50),  
				email varchar2(25) unique,--唯一  
				tel number(11)
				-- deptno number(2) references dept(deptno) -- 外键  
				)`,
			want: "CREATE TABLE `TEST`.`USERINFO` (`ID` INT PRIMARY KEY,`NAME` VARCHAR(20) NOT NULL,`SEX` TINYINT,`AGE` SMALLINT,`BIRTHDAY` DATETIME,`ADDRESS` VARCHAR(50),`EMAIL` VARCHAR(25) UNIQUE KEY,`TEL` BIGINT) DEFAULT CHARACTER SET = UTF8MB4"},

		{
			name: "createOutOfLineConstraint",
			sql: `CREATE TABLE TEST.employees_demo
				( employee_id    NUMBER(6)
				, first_name     VARCHAR2(20)
				, last_name      VARCHAR2(25)
					 CONSTRAINT emp_last_name_nn_demo NOT NULL
				, email          VARCHAR2(25)
					 CONSTRAINT emp_email_nn_demo     NOT NULL
				, phone_number   VARCHAR2(20)
				, hire_date      DATE
					 CONSTRAINT emp_hire_date_nn_demo  NOT NULL
				, job_id         VARCHAR2(10)
				   CONSTRAINT     emp_job_nn_demo  NOT NULL
				, salary         NUMBER(8,2)
				   CONSTRAINT     emp_salary_nn_demo  NOT NULL
				, commission_pct NUMBER(2,2)
				, manager_id     NUMBER(6)
				, department_id  NUMBER(4)
				, dn             VARCHAR2(300)
				, CONSTRAINT     emp_email_uk_demo
								 UNIQUE (email)
				)`,
			want: "CREATE TABLE `TEST`.`EMPLOYEES_DEMO` (`EMPLOYEE_ID` INT,`FIRST_NAME` VARCHAR(20),`LAST_NAME` VARCHAR(25) NOT NULL,`EMAIL` VARCHAR(25) NOT NULL,`PHONE_NUMBER` VARCHAR(20),`HIRE_DATE` DATETIME NOT NULL,`JOB_ID` VARCHAR(10) NOT NULL,`SALARY` DECIMAL(8,2) NOT NULL,`COMMISSION_PCT` DECIMAL(2,2),`MANAGER_ID` INT,`DEPARTMENT_ID` SMALLINT,`DN` VARCHAR(300),UNIQUE `EMP_EMAIL_UK_DEMO`(`email`)) DEFAULT CHARACTER SET = UTF8MB4",
		},
		// {
		// 	name: "createOutOfLineConstraint",
		// 	sql:  `CREATE TABLE TEST.emp1 ( id number REFERENCES TEST.USERINFO11 ( ID ), NAME VARCHAR ( 8 ) );`,
		// 	want: "CREATE TABLE `TEST`.`EMP1` (`ID` DOUBLE,`NAME` VARCHAR(8)) DEFAULT CHARACTER SET = UTF8MB4",
		// },
	}
	logger := hclog.NewNullLogger()
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := extractor.parseDDLSQL(tt.sql, "")
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
