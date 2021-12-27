package extractor

import (
	"testing"

	"github.com/actiontech/dtle/drivers/mysql/common"

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
		name              string
		sql               string
		undo_sql          string
		want_where_values []interface{}
		want_new_values   []interface{}
	}{
		// NUMBER(*)
		// BFILE  no support
		// BINARY_FLOAT
		// INSERT INTO TEST.TEST("COL1","COL2") VALUES (1, 3.40282E+38F);
		// INSERT INTO TEST.TEST("COL1","COL2") VALUES (2, BINARY_FLOAT_INFINITY);
		// INSERT INTO TEST.TEST("COL1","COL2") VALUES (3, -BINARY_FLOAT_INFINITY);
		// INSERT INTO TEST.TEST("COL1","COL2") VALUES (4, BINARY_FLOAT_NAN);
		{
			name:              "BINARY_FLOAT",
			sql:               `INSERT INTO "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('0', '1.17549E-38F');`,
			undo_sql:          ``,
			want_where_values: []interface{}{},
			want_new_values:   []interface{}{"0", "1.17549E-38F"},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `INSERT INTO TEST.BINARY_FLOAT("COL1","COL2") VALUES ('1', '3.40282E+38F');`,
			undo_sql:          ``,
			want_where_values: []interface{}{},
			want_new_values:   []interface{}{"1", "3.40282E+38F"},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `INSERT INTO "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('2', 'Inf');`,
			undo_sql:          ``,
			want_where_values: []interface{}{},
			want_new_values:   []interface{}{"2", nil},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `INSERT INTO "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('3', '-Inf');`,
			undo_sql:          ``,
			want_where_values: []interface{}{},
			want_new_values:   []interface{}{"3", nil},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `INSERT INTO "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('4', 'Nan');`,
			undo_sql:          ``,
			want_where_values: []interface{}{},
			want_new_values:   []interface{}{"4", nil},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `UPDATE "TEST"."BINARY_FLOAT" SET "COL2" ='500'  WHERE "COL1" = '3' AND "COL2" = "NULL";`,
			undo_sql:          `UPDATE "TEST"."BINARY_FLOAT" SET "COL2" = NULL  WHERE "COL1" = '3' AND "COL2" = '500';`,
			want_where_values: []interface{}{"3", nil},
			want_new_values:   []interface{}{"3", "500"},
		},
		{
			name:              "BINARY_FLOAT",
			sql:               `DELETE FROM "TEST"."BINARY_FLOAT" WHERE "COL1" = '4' AND "COL2" = "Nan";`,
			undo_sql:          `INSERT INTO "TEST"."BINARY_FLOAT"("COL1","COL2") VALUES ('4', 'Nan');`,
			want_where_values: []interface{}{"4", nil},
			want_new_values:   []interface{}{},
		},
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

			for i := 0; i < len(dataEvent.WhereColumnValues.AbstractValues); i++ {
				if dataEvent.DML == common.UpdateDML || dataEvent.DML == common.DeleteDML {
					if dataEvent.WhereColumnValues.AbstractValues[i] != tt.want_where_values[i] {
						t.Errorf("parseDMLSQL() where index %v value = %v, want %v", i, dataEvent.WhereColumnValues.AbstractValues, tt.want_where_values)
					}
				}
				if dataEvent.DML == common.UpdateDML || dataEvent.DML == common.InsertDML {
					if dataEvent.NewColumnValues.AbstractValues[i] != tt.want_new_values[i] {
						t.Errorf("parseDMLSQL() new index %v value = %v, want %v", i, dataEvent.NewColumnValues.AbstractValues, tt.want_new_values)
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
			want: "CREATE TABLE `TEST`.`NUMBER_WILDCARD_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BFILE_COLUMNS(ID INT, C_BFILE BFILE);`,
			want: "CREATE TABLE `TEST`.`BFILE_COLUMNS` (`ID` INT,`C_BFILE` VARCHAR(255)) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BINARY_FLOAT_COLUMNS(ID INT, C_BINARY_FLOAT BINARY_FLOAT);`,
			want: "CREATE TABLE `TEST`.`BINARY_FLOAT_COLUMNS` (`ID` INT,`C_BINARY_FLOAT` FLOAT) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BINARY_DOUBLE_COLUMNS(ID INT, C_BINARY_DOUBLE BINARY_DOUBLE);`,
			want: "CREATE TABLE `TEST`.`BINARY_DOUBLE_COLUMNS` (`ID` INT,`C_BINARY_DOUBLE` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.BLOB_COLUMNS(ID INT, C_BLOB BLOB);`,
			want: "CREATE TABLE `TEST`.`BLOB_COLUMNS` (`ID` INT,`C_BLOB` LONGBLOB) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHAR_255_COLUMNS(ID INT, C_CHAR CHAR(255));`,
			want: "CREATE TABLE `TEST`.`CHAR_255_COLUMNS` (`ID` INT,`C_CHAR` CHAR(255)) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHARACTER_255_COLUMNS(ID INT, C_CHARACTER CHARACTER(255));`,
			want: "CREATE TABLE `TEST`.`CHARACTER_255_COLUMNS` (`ID` INT,`C_CHARACTER` CHAR(255)) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CHARACTER_256_COLUMNS(ID INT, C_CHARACTER CHARACTER(256));`,
			want: "CREATE TABLE `TEST`.`CHARACTER_256_COLUMNS` (`ID` INT,`C_CHARACTER` VARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.CLOB_COLUMNS(ID INT, C_CLOB CLOB);`,
			want: "CREATE TABLE `TEST`.`CLOB_COLUMNS` (`ID` INT,`C_CLOB` LONGTEXT) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DATE_COLUMNS(ID INT, C_DATE DATE);`,
			want: "CREATE TABLE `TEST`.`DATE_COLUMNS` (`ID` INT,`C_DATE` DATETIME) DEFAULT CHARACTER SET=utf8"},
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DECIMAL_COLUMNS(ID INT, C_DECIMAL DECIMAL(11, 3));`,
			want: "CREATE TABLE `TEST`.`DECIMAL_COLUMNS` (`ID` INT,`C_DECIMAL` DECIMAL(11,3)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.DEC_COLUMNS(ID INT, C_DEC DEC(11, 3));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DEC_COLUMNS(ID INT, C_DEC DEC(11, 3));`,
			want: "CREATE TABLE `TEST`.`DEC_COLUMNS` (`ID` INT,`C_DEC` DEC(11,3)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.DOUBLE_PRECISION_COLUMNS(ID INT, C_DOUBLE_PRECISION DOUBLE PRECISION);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.DOUBLE_PRECISION_COLUMNS(ID INT, C_DOUBLE_PRECISION DOUBLE PRECISION);`,
			want: "CREATE TABLE `TEST`.`DOUBLE_PRECISION_COLUMNS` (`ID` INT,`C_DOUBLE_PRECISION` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.FLOAT_COLUMNS(ID INT, C_FLOAT FLOAT(11));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.FLOAT_COLUMNS(ID INT, C_FLOAT FLOAT(11));`,
			want: "CREATE TABLE `TEST`.`FLOAT_COLUMNS` (`ID` INT,`C_FLOAT` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.INTEGER_COLUMNS(ID INT, C_INTEGER INTEGER);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTEGER_COLUMNS(ID INT, C_INTEGER INTEGER);`,
			want: "CREATE TABLE `TEST`.`INTEGER_COLUMNS` (`ID` INT,`C_INTEGER` INT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.INT_COLUMNS(ID INT, C_INT INT);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INT_COLUMNS(ID INT, C_INT INT);`,
			want: "CREATE TABLE `TEST`.`INT_COLUMNS` (`ID` INT,`C_INT` INT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.INTERVAL_YEAR_COLUMNS(ID INT, C_INTERVAL_YEAR INTERVAL YEAR(3) TO MONTH);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTERVAL_YEAR_COLUMNS(ID INT, C_INTERVAL_YEAR INTERVAL YEAR(3) TO MONTH);`,
			want: "CREATE TABLE `TEST`.`INTERVAL_YEAR_COLUMNS` (`ID` INT,`C_INTERVAL_YEAR` VARCHAR(30)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.INTERVAL_DAY_COLUMNS(ID INT, C_INTERVAL_DAY INTERVAL DAY(3) TO SECOND(5));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.INTERVAL_DAY_COLUMNS(ID INT, C_INTERVAL_DAY INTERVAL DAY(3) TO SECOND(5));`,
			want: "CREATE TABLE `TEST`.`INTERVAL_DAY_COLUMNS` (`ID` INT,`C_INTERVAL_DAY` VARCHAR(30)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.LONG_COLUMNS(ID INT, C_LONG LONG);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.LONG_COLUMNS(ID INT, C_LONG LONG);`,
			want: "CREATE TABLE `TEST`.`LONG_COLUMNS` (`ID` INT,`C_LONG` LONGTEXT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.LONG_RAW_COLUMNS(ID INT, C_LONG_RAW LONG RAW);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.LONG_RAW_COLUMNS(ID INT, C_LONG_RAW LONG RAW);`,
			want: "CREATE TABLE `TEST`.`LONG_RAW_COLUMNS` (`ID` INT,`C_LONG_RAW` LONGBLOB) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NCHAR_255_COLUMNS(ID INT, C_NCHAR NCHAR(255));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_255_COLUMNS(ID INT, C_NCHAR NCHAR(255));`,
			want: "CREATE TABLE `TEST`.`NCHAR_255_COLUMNS` (`ID` INT,`C_NCHAR` NCHAR(255)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NCHAR_256_COLUMNS(ID INT, C_NCHAR NCHAR(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_256_COLUMNS(ID INT, C_NCHAR NCHAR(256));`,
			want: "CREATE TABLE `TEST`.`NCHAR_256_COLUMNS` (`ID` INT,`C_NCHAR` NVARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NCHAR_VARYING_COLUMNS(ID INT, C_NCHAR_VARYING NCHAR VARYING(2000));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCHAR_VARYING_COLUMNS(ID INT, C_NCHAR_VARYING NCHAR VARYING(2000));`,
			want: "CREATE TABLE `TEST`.`NCHAR_VARYING_COLUMNS` (`ID` INT,`C_NCHAR_VARYING` NVARCHAR(2000)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NCLOB_COLUMNS(ID INT, C_NCLOB NCLOB);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NCLOB_COLUMNS(ID INT, C_NCLOB NCLOB);`,
			want: "CREATE TABLE `TEST`.`NCLOB_COLUMNS` (`ID` INT,`C_NCLOB` TEXT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_2_COLUMNS(ID INT, C_NUMBER NUMBER(2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_2_COLUMNS(ID INT, C_NUMBER NUMBER(2));`,
			want: "CREATE TABLE `TEST`.`NUMBER_2_COLUMNS` (`ID` INT,`C_NUMBER` TINYINT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_4_COLUMNS(ID INT, C_NUMBER NUMBER(4, 0));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_4_COLUMNS(ID INT, C_NUMBER NUMBER(4, 0));`,
			want: "CREATE TABLE `TEST`.`NUMBER_4_COLUMNS` (`ID` INT,`C_NUMBER` SMALLINT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_8_COLUMNS(ID INT, C_NUMBER NUMBER(8));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_8_COLUMNS(ID INT, C_NUMBER NUMBER(8));`,
			want: "CREATE TABLE `TEST`.`NUMBER_8_COLUMNS` (`ID` INT,`C_NUMBER` INT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_18_COLUMNS(ID INT, C_NUMBER NUMBER(18, 0));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_18_COLUMNS(ID INT, C_NUMBER NUMBER(18, 0));`,
			want: "CREATE TABLE `TEST`.`NUMBER_18_COLUMNS` (`ID` INT,`C_NUMBER` BIGINT) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38));`,
			want: "CREATE TABLE `TEST`.`NUMBER_38_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(38)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38,31));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_38_COLUMNS(ID INT, C_NUMBER NUMBER(38,31));`,
			want: "CREATE TABLE `TEST`.`NUMBER_38_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(38,30)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_8_2_COLUMNS(ID INT, C_NUMBER NUMBER(8, 2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_8_2_COLUMNS(ID INT, C_NUMBER NUMBER(8, 2));`,
			want: "CREATE TABLE `TEST`.`NUMBER_8_2_COLUMNS` (`ID` INT,`C_NUMBER` DECIMAL(8,2)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_COLUMNS(ID INT, C_NUMBER NUMBER);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_COLUMNS(ID INT, C_NUMBER NUMBER);`,
			want: "CREATE TABLE `TEST`.`NUMBER_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMBER_WILDCARD_COLUMNS(ID INT, C_NUMBER NUMBER(*));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMBER_WILDCARD_COLUMNS(ID INT, C_NUMBER NUMBER(*));`,
			want: "CREATE TABLE `TEST`.`NUMBER_WILDCARD_COLUMNS` (`ID` INT,`C_NUMBER` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NUMERIC_COLUMNS(ID INT, C_NUMERIC NUMERIC(8, 2));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NUMERIC_COLUMNS(ID INT, C_NUMERIC NUMERIC(8, 2));`,
			want: "CREATE TABLE `TEST`.`NUMERIC_COLUMNS` (`ID` INT,`C_NUMERIC` NUMERIC(8,2)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.NVARCHAR2_COLUMNS(ID INT, C_NVARCHAR2 NVARCHAR2(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.NVARCHAR2_COLUMNS(ID INT, C_NVARCHAR2 NVARCHAR2(256));`,
			want: "CREATE TABLE `TEST`.`NVARCHAR2_COLUMNS` (`ID` INT,`C_NVARCHAR2` NVARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.RAW_COLUMNS(ID INT, C_RAW RAW(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.RAW_COLUMNS(ID INT, C_RAW RAW(256));`,
			want: "CREATE TABLE `TEST`.`RAW_COLUMNS` (`ID` INT,`C_RAW` VARBINARY(256)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.REAL_COLUMNS(ID INT, C_REAL REAL);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.REAL_COLUMNS(ID INT, C_REAL REAL);`,
			want: "CREATE TABLE `TEST`.`REAL_COLUMNS` (`ID` INT,`C_REAL` DOUBLE) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.ROWID_COLUMNS(ID INT, C_ROWID ROWID);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.ROWID_COLUMNS(ID INT, C_ROWID ROWID);`,
			want: "CREATE TABLE `TEST`.`ROWID_COLUMNS` (`ID` INT,`C_ROWID` CHAR(100)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.SMALLINT_COLUMNS(ID INT, C_SMALLINT SMALLINT);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.SMALLINT_COLUMNS(ID INT, C_SMALLINT SMALLINT);`,
			want: "CREATE TABLE `TEST`.`SMALLINT_COLUMNS` (`ID` INT,`C_SMALLINT` DECIMAL(38)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.TIMESTAMP_COLUMNS(ID INT, C_TIMESTAMP TIMESTAMP(9));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.TIMESTAMP_COLUMNS(ID INT, C_TIMESTAMP TIMESTAMP(9));`,
			want: "CREATE TABLE `TEST`.`TIMESTAMP_COLUMNS` (`ID` INT,`C_TIMESTAMP` DATETIME(6)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.TIMESTAMP_ZONE_COLUMNS(ID INT, C_TIMESTAMP_ZONE TIMESTAMP(9) WITH TIME ZONE);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.TIMESTAMP_ZONE_COLUMNS(ID INT, C_TIMESTAMP_ZONE TIMESTAMP(9) WITH TIME ZONE);`,
			want: "CREATE TABLE `TEST`.`TIMESTAMP_ZONE_COLUMNS` (`ID` INT,`C_TIMESTAMP_ZONE` DATETIME(6)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.UROWID_COLUMNS(ID INT, C_UROWID UROWID(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.UROWID_COLUMNS(ID INT, C_UROWID UROWID(256));`,
			want: "CREATE TABLE `TEST`.`UROWID_COLUMNS` (`ID` INT,`C_UROWID` VARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.VARCHAR_COLUMNS(ID INT, C_VARCHAR VARCHAR(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.VARCHAR_COLUMNS(ID INT, C_VARCHAR VARCHAR(256));`,
			want: "CREATE TABLE `TEST`.`VARCHAR_COLUMNS` (`ID` INT,`C_VARCHAR` VARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		//CREATE TABLE TEST.VARCHAR2_COLUMNS(ID INT, C_VARCHAR2 VARCHAR2(256));
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.VARCHAR2_COLUMNS(ID INT, C_VARCHAR2 VARCHAR2(256));`,
			want: "CREATE TABLE `TEST`.`VARCHAR2_COLUMNS` (`ID` INT,`C_VARCHAR2` VARCHAR(256)) DEFAULT CHARACTER SET=utf8"},
		// CREATE TABLE TEST.XMLTYPE_COLUMNS(ID INT, C_XMLTYPE XMLTYPE);
		{
			name: "createTableSQLCharRelation",
			sql:  `CREATE TABLE TEST.XMLTYPE_COLUMNS(ID INT, C_XMLTYPE XMLTYPE);`,
			want: "CREATE TABLE `TEST`.`XMLTYPE_COLUMNS` (`ID` INT,`C_XMLTYPE` LONGTEXT) DEFAULT CHARACTER SET=utf8"},

		// {
		// 	name: "createTableSQLCharRelation",
		// 	sql: `CREATE TABLE test."persons"(
		// 		   "first_name" VARCHAR(15) NOT NULL,
		// 		   last_name VARCHAR2(45) NOT NULL
		// 		 );`,
		// 	want: "CREATE TABLE `TEST`.`persons (first_name VARCHAR(15),LAST_NAME VARCHAR(45)) DEFAULT CHARACTER SET=utf8"},
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
		// 	want: "CREATE TABLE `TEST`.`persons (first_num DECIMAL(15,2),SECOND_NUM BIGINT,THREE_NUM INT,LAST_NAME DOUBLE,NUMERIC_NAME NUMERIC(15,2),DECIMAL_NAME DECIMAL(15,2),DEC_NAME DEC(15,2),INTEGER_NAME INT,INT_NAME INT,SMALLINT_NAME DECIMAL(38)) DEFAULT CHARACTER SET=utf8"},
	}
	logger := hclog.NewNullLogger()
	extractor := &ExtractorOracle{logger: logger, replicateDoDb: []*common.DataSource{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := extractor.parseDDLSQL(tt.sql)
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
			want: "ALTER TABLE `TEST`.`persons` ADD COLUMN(`ADD_NAME1` VARCHAR(30)),ADD COLUMN(`ADD_NAME2` VARCHAR(30))"},
		{
			name: "alterTableSQLModify",
			sql: `ALTER TABLE test."persons" MODIFY (
					alter_new_name1 CHAR ( 13 )) MODIFY (
					alter_name2 VARCHAR ( 66 ))`,
			want: "ALTER TABLE `TEST`.`persons` MODIFY `ALTER_NEW_NAME1` CHAR(13),MODIFY `ALTER_NAME2` VARCHAR(66)"},
		{
			name: "alterTableSQLDrop",
			sql:  `ALTER TABLE "TEST"."persons" DROP ("DROP_NAME1",drop_name2)`,
			want: "ALTER TABLE `TEST`.`persons` DROP COLUMN `DROP_NAME1`,DROP COLUMN `DROP_NAME2`"},
		{
			name: "alterTableSQLRename",
			sql:  `ALTER TABLE "TEST"."persons" RENAME COLUMN "RE_NAME" TO "RE_NAME_NEW"`,
			want: "ALTER TABLE `TEST`.`persons` RENAME COLUMN `RE_NAME` TO `RE_NAME_NEW`"},
		// index
	}
	extractor := &ExtractorOracle{logger: logger}
	for _, tt := range testAlter {
		t.Run(tt.name, func(t *testing.T) {
			dataEvent, err := extractor.parseDDLSQL(tt.sql)
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
			dataEvent, err := extractor.parseDDLSQL(tt.sql)
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
