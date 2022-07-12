/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package sql

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/mysql/mysqlconfig"
	test "github.com/outbrain/golib/tests"
)

var (
	spacesRegexp = regexp.MustCompile(`[ \t\n\r]+`)
)

func normalizeQuery(name string) string {
	name = strings.Replace(name, "`", "", -1)
	name = spacesRegexp.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	return name
}

func TestBuildDMLInsertQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	{
		uniqueKeyColumns := common.NewColumnList([]mysqlconfig.Column{
			{
				RawName:            "id",
				EscapedName:        "id",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "name",
				EscapedName:        "name",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "rank",
				EscapedName:        "rank",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "position",
				EscapedName:        "position",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "age",
				EscapedName:        "age",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			},
		})
		args := [][]interface{}{{3, "testName", "first", 17, 23}}

		query, explodedArgs, err := BuildDMLInsertQuery(databaseName, tableName, uniqueKeyColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `replace into mydb.tbl  values (?,?,?,?,?)`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, "testName", "first", 17, 23}))
	}
}

func TestBuildDMLInsertQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := common.NewColumnList([]mysqlconfig.Column{{
		RawName:            "id",
		EscapedName:        "id",
		IsUnsigned:         false,
		Charset:            "",
		Type:               0,
		Default:            nil,
		ColumnType:         "",
		Key:                "",
		TimezoneConversion: nil,
		Nullable:           false,
		Precision:          0,
		Scale:              0,
	}, {
		RawName:            "name",
		EscapedName:        "name",
		IsUnsigned:         false,
		Charset:            "",
		Type:               0,
		Default:            nil,
		ColumnType:         "",
		Key:                "",
		TimezoneConversion: nil,
		Nullable:           false,
		Precision:          0,
		Scale:              0,
	}, {
		RawName:            "rank",
		EscapedName:        "rank",
		IsUnsigned:         false,
		Charset:            "",
		Type:               0,
		Default:            nil,
		ColumnType:         "",
		Key:                "",
		TimezoneConversion: nil,
		Nullable:           false,
		Precision:          0,
		Scale:              0,
	}, {
		RawName:            "position",
		EscapedName:        "position",
		IsUnsigned:         false,
		Charset:            "",
		Type:               0,
		Default:            nil,
		ColumnType:         "",
		Key:                "PRI",
		TimezoneConversion: nil,
		Nullable:           false,
		Precision:          0,
		Scale:              0,
	}, {
		RawName:            "age",
		EscapedName:        "age",
		IsUnsigned:         false,
		Charset:            "",
		Type:               0,
		Default:            nil,
		ColumnType:         "",
		Key:                "",
		TimezoneConversion: nil,
		Nullable:           false,
		Precision:          0,
		Scale:              0,
	},
	})
	{
		// testing signed
		args := [][]interface{}{{3, "testname", "first", int8(-1), 23}}
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `replace into mydb.tbl  values (?,?,?,?,?)`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "first", int8(-1), 23}))
	}
	{
		// testing unsigned
		args := [][]interface{}{{3, "testname", "first", int8(-1), 23}}
		tableColumns.SetUnsigned("position")
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			replace into mydb.tbl  values (?,?,?,?,?)`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "first", uint8(255), 23}))
	}
	{
		// testing unsigned
		args := [][]interface{}{{3, "testname", "first", int32(-1), 23}}
		tableColumns.SetUnsigned("position")
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			replace into mydb.tbl  values (?,?,?,?,?)`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "first", uint32(4294967295), 23}))
	}
}

func TestBuildDMLDeleteQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"

	args := []interface{}{3, "testname", "first", 17, 23}
	tableColumns := common.NewColumnList([]mysqlconfig.Column{
		{
			RawName:            "id",
			EscapedName:        "id",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "name",
			EscapedName:        "name",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "rank",
			EscapedName:        "rank",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "position",
			EscapedName:        "position",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "PRI",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "age",
			EscapedName:        "age",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		},
	})
	{
		query, uniqueKeyArgs, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			delete  from
				mydb.tbl
				where
					((position = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17}))
	}
	{
		tableColumns := common.NewColumnList([]mysqlconfig.Column{
			{
				RawName:            "id",
				EscapedName:        "id",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "name",
				EscapedName:        "name",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "rank",
				EscapedName:        "rank",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "position",
				EscapedName:        "position",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "age",
				EscapedName:        "age",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			},
		})
		query, uniqueKeyArgs, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			delete	from
				mydb.tbl
				where
					((name = ?) and (position = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{"testname", 17}))
	}
	{
		tableColumns := common.NewColumnList([]mysqlconfig.Column{
			{
				RawName:            "position",
				EscapedName:        "position",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "name",
				EscapedName:        "name",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "id",
				EscapedName:        "id",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "rank",
				EscapedName:        "rank",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "age",
				EscapedName:        "age",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			},
		})
		query, uniqueKeyArgs, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			delete
					from
					mydb.tbl
				where
					((position = ?) and (name = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{3, "testname"}))
	}
	{
		args := []interface{}{"first", 17}

		_, _, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNotNil(err)
	}
}

func TestBuildDMLDeleteQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := common.NewColumnList([]mysqlconfig.Column{
		{
			RawName:            "position",
			EscapedName:        "position",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "PRI",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "name",
			EscapedName:        "name",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "id",
			EscapedName:        "id",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "rank",
			EscapedName:        "rank",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "age",
			EscapedName:        "age",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		},
	})
	{
		// test signed (expect no change)
		args := []interface{}{-1, "testname", "first", 3, 23}
		query, uniqueKeyArgs, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			delete
				from
					mydb.tbl
				where
					((position = ?))
 				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{-1}))
	}
	{
		// test unsigned
		args := []interface{}{int8(-1), "testname", "first", 3, 23}
		tableColumns.SetUnsigned("position")
		query, uniqueKeyArgs, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, []string{}, args, nil)
		test.S(t).ExpectNil(err)
		expected := `
			delete 
				from
					mydb.tbl
				where
					((position = ?))
 				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{uint8(255)}))
	}
}

func TestBuildDMLUpdateQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := common.NewColumnList([]mysqlconfig.Column{
		{
			RawName:            "id",
			EscapedName:        "id",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "name",
			EscapedName:        "name",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "rank",
			EscapedName:        "rank",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "position",
			EscapedName:        "position",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "PRI",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		}, {
			RawName:            "age",
			EscapedName:        "age",
			IsUnsigned:         false,
			Charset:            "",
			Type:               0,
			Default:            nil,
			ColumnType:         "",
			Key:                "",
			TimezoneConversion: nil,
			Nullable:           false,
			Precision:          0,
			Scale:              0,
		},
	})
	valueArgs := []interface{}{3, "testname", "newval", 17, 23}
	whereArgs := []interface{}{3, "testname", "findme", 17, 56}
	{
		query, sharedArgs, uniqueKeyArgs, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, []string{}, valueArgs, whereArgs, nil)
		test.S(t).ExpectNil(err)
		expected := `
			update 
			  mydb.tbl
					set id=?, name=?, rank=?, position=?, age=?
				where
					((position = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "newval", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17}))
	}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{"position", "name"})
	//	query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNil(err)
	//	expected := `
	//		update /* udup mydb.tbl */
	//		  mydb.tbl
	//				set id=?, name=?, position=?, age=?
	//			where
	//				((position = ?) and (name = ?))
	//	`
	//	test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17, "testname"}))
	//}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{"age"})
	//	query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNil(err)
	//	expected := `
	//		update /* udup mydb.tbl */
	//		  mydb.tbl
	//				set id=?, name=?, position=?, age=?
	//			where
	//				((age = ?))
	//	`
	//	test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{56}))
	//}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{"age", "position", "id", "name"})
	//	query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNil(err)
	//	expected := `
	//		update /* udup mydb.tbl */
	//		  mydb.tbl
	//				set id=?, name=?, position=?, age=?
	//			where
	//				((age = ?) and (position = ?) and (id = ?) and (name = ?))
	//	`
	//	test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{56, 17, 3, "testname"}))
	//}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{"age", "surprise"})
	//	_, _, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNotNil(err)
	//}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{})
	//	_, _, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNotNil(err)
	//}
	//{
	//	sharedColumns := common.NewColumnList([]string{"id", "name", "position", "age"})
	//	mappedColumns := common.NewColumnList([]string{"id", "name", "role", "age"})
	//	uniqueKeyColumns := common.NewColumnList([]string{"id"})
	//	query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, mappedColumns, uniqueKeyColumns, valueArgs, whereArgs)
	//	test.S(t).ExpectNil(err)
	//	expected := `
	//		update /* udup mydb.tbl */
	//		  mydb.tbl
	//				set id=?, name=?, role=?, age=?
	//			where
	//				((id = ?))
	//	`
	//	test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
	//	test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{3}))
	//}
}

func TestBuildDMLUpdateQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"

	valueArgs := []interface{}{3, "testname", "newval", int8(-17), int8(-2)}
	whereArgs := []interface{}{3, "testname", "findme", int8(-3), 56}
	{
		tableColumns := common.NewColumnList([]mysqlconfig.Column{
			{
				RawName:            "id",
				EscapedName:        "id",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "name",
				EscapedName:        "name",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "rank",
				EscapedName:        "rank",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "position",
				EscapedName:        "position",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "age",
				EscapedName:        "age",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			},
		})
		query, sharedArgs, uniqueKeyArgs, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, []string{}, valueArgs, whereArgs, nil)
		test.S(t).ExpectNil(err)
		expected := `
			update
					mydb.tbl
				set
					id=?, name=?, rank=?, position=?, age=?
				where
					((position = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "newval", int8(-17), int8(-2)}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{int8(-3)}))
	}
	{
		tableColumns := common.NewColumnList([]mysqlconfig.Column{
			{
				RawName:            "id",
				EscapedName:        "id",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "name",
				EscapedName:        "name",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "rank",
				EscapedName:        "rank",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "position",
				EscapedName:        "position",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "PRI",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			}, {
				RawName:            "age",
				EscapedName:        "age",
				IsUnsigned:         false,
				Charset:            "",
				Type:               0,
				Default:            nil,
				ColumnType:         "",
				Key:                "",
				TimezoneConversion: nil,
				Nullable:           false,
				Precision:          0,
				Scale:              0,
			},
		})
		// test unsigned
		tableColumns.SetUnsigned("age")
		tableColumns.SetUnsigned("position")
		query, sharedArgs, uniqueKeyArgs, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, []string{}, valueArgs, whereArgs, nil)
		test.S(t).ExpectNil(err)
		expected := `
			update
			  mydb.tbl
				set
					id=?, name=?, rank=?, position=?, age=?
				where
					((position = ?))
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", "newval", uint8(239), uint8(254)}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{uint8(253)}))
	}
}
