/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/actiontech/kafkas, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package sql

import (
	"testing"

	"github.com/actiontech/dtle/olddtle/internal/config/mysql"

	"reflect"
	"regexp"
	"strings"

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

func TestEscapeName(t *testing.T) {
	names := []string{"my_table", `"my_table"`, "`my_table`"}
	for _, name := range names {
		escaped := mysql.EscapeName(name)
		test.S(t).ExpectEquals(escaped, "`my_table`")
	}
}

func TestBuildSetPreparedClause(t *testing.T) {
	{
		columns := NewColumnList([]string{"c1"})
		clause, err := BuildSetPreparedClause(columns)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(clause, "`c1`=?")
	}
	{
		columns := NewColumnList([]string{"c1", "c2"})
		clause, err := BuildSetPreparedClause(columns)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(clause, "`c1`=?, `c2`=?")
	}
	{
		columns := NewColumnList([]string{})
		_, err := BuildSetPreparedClause(columns)
		test.S(t).ExpectNotNil(err)
	}
}

func TestBuildRangeInsertQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	{
		uniqueKey := "PRIMARY"
		uniqueKeyColumns := NewColumnList([]string{"id"})
		rangeStartValues := []string{"@v1s"}
		rangeEndValues := []string{"@v1e"}
		rangeStartArgs := []interface{}{3}
		rangeEndArgs := []interface{}{103}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, false)
		test.S(t).ExpectNil(err)
		expected := `
				insert /* udup mydb.tbl */ ignore into mydb.ghost (id, name, position)
				(select id, name, position from mydb.tbl force index (PRIMARY)
					where (((id > @v1s) or ((id = @v1s))) and ((id < @v1e) or ((id = @v1e))))
				)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 103, 103}))
	}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartValues := []string{"@v1s", "@v2s"}
		rangeEndValues := []string{"@v1e", "@v2e"}
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, false)
		test.S(t).ExpectNil(err)
		expected := `
				insert /* udup mydb.tbl */ ignore into mydb.ghost (id, name, position)
				(select id, name, position from mydb.tbl force index (name_position_uidx)
				  where (((name > @v1s) or (((name = @v1s)) AND (position > @v2s)) or ((name = @v1s) and (position = @v2s))) and ((name < @v1e) or (((name = @v1e)) AND (position < @v2e)) or ((name = @v1e) and (position = @v2e))))
				)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}))
	}
}

func TestBuildRangeInsertQueryRenameMap(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	mappedSharedColumns := []string{"id", "name", "location"}
	{
		uniqueKey := "PRIMARY"
		uniqueKeyColumns := NewColumnList([]string{"id"})
		rangeStartValues := []string{"@v1s"}
		rangeEndValues := []string{"@v1e"}
		rangeStartArgs := []interface{}{3}
		rangeEndArgs := []interface{}{103}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, false)
		test.S(t).ExpectNil(err)
		expected := `
				insert /* udup mydb.tbl */ ignore into mydb.ghost (id, name, location)
				(select id, name, position from mydb.tbl force index (PRIMARY)
					where (((id > @v1s) or ((id = @v1s))) and ((id < @v1e) or ((id = @v1e))))
				)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 103, 103}))
	}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartValues := []string{"@v1s", "@v2s"}
		rangeEndValues := []string{"@v1e", "@v2e"}
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertQuery(databaseName, originalTableName, ghostTableName, sharedColumns, mappedSharedColumns, uniqueKey, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, true, false)
		test.S(t).ExpectNil(err)
		expected := `
				insert /* udup mydb.tbl */ ignore into mydb.ghost (id, name, location)
				(select id, name, position from mydb.tbl force index (name_position_uidx)
				  where (((name > @v1s) or (((name = @v1s)) AND (position > @v2s)) or ((name = @v1s) and (position = @v2s))) and ((name < @v1e) or (((name = @v1e)) AND (position < @v2e)) or ((name = @v1e) and (position = @v2e))))
				)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}))
	}
}

func TestBuildRangeInsertPreparedQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	ghostTableName := "ghost"
	sharedColumns := []string{"id", "name", "position"}
	{
		uniqueKey := "name_position_uidx"
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildRangeInsertPreparedQuery(databaseName, originalTableName, ghostTableName, sharedColumns, sharedColumns, uniqueKey, uniqueKeyColumns, rangeStartArgs, rangeEndArgs, true, true)
		test.S(t).ExpectNil(err)
		expected := `
				insert /* udup mydb.tbl */ ignore into mydb.ghost (id, name, position)
				(select id, name, position from mydb.tbl force index (name_position_uidx)
				  where (((name > ?) or (((name = ?)) AND (position > ?)) or ((name = ?) and (position = ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?))))
				lock in share mode )
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 17, 3, 17, 103, 103, 117, 103, 117}))
	}
}

func TestBuildUniqueKeyRangeEndPreparedQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	var chunkSize int64 = 500
	{
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})
		rangeStartArgs := []interface{}{3, 17}
		rangeEndArgs := []interface{}{103, 117}

		query, explodedArgs, err := BuildUniqueKeyRangeEndPreparedQuery(databaseName, originalTableName, uniqueKeyColumns, rangeStartArgs, rangeEndArgs, chunkSize, false, "test")
		test.S(t).ExpectNil(err)
		expected := `
				select /* udup mydb.tbl test */ name, position
				  from (
				    select
				        name, position
				      from
				        mydb.tbl
				      where ((name > ?) or (((name = ?)) AND (position > ?))) and ((name < ?) or (((name = ?)) AND (position < ?)) or ((name = ?) and (position = ?)))
				      order by
				        name asc, position asc
				      limit 500
				  ) select_osc_chunk
				order by
				  name desc, position desc
				limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(explodedArgs, []interface{}{3, 3, 17, 103, 103, 117, 103, 117}))
	}
}

func TestBuildUniqueKeyMinValuesPreparedQuery(t *testing.T) {
	databaseName := "mydb"
	originalTableName := "tbl"
	uniqueKeyColumns := NewColumnList([]string{"name", "position"})
	{
		query, err := BuildUniqueKeyMinValuesPreparedQuery(databaseName, originalTableName, uniqueKeyColumns)
		test.S(t).ExpectNil(err)
		expected := `
			select /* udup mydb.tbl */ name, position
			  from
			    mydb.tbl
			  order by
			    name asc, position asc
			  limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	}
	{
		query, err := BuildUniqueKeyMaxValuesPreparedQuery(databaseName, originalTableName, uniqueKeyColumns)
		test.S(t).ExpectNil(err)
		expected := `
			select /* udup mydb.tbl */ name, position
			  from
			    mydb.tbl
			  order by
			    name desc, position desc
			  limit 1
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
	}
}

func TestBuildDMLDeleteQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	args := []interface{}{3, "testname", "first", 17, 23}
	{
		uniqueKeyColumns := NewColumnList([]string{"position"})

		query, uniqueKeyArgs, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			delete /* udup mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17}))
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"name", "position"})

		query, uniqueKeyArgs, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			delete /* udup mydb.tbl */
				from
					mydb.tbl
				where
					((name = ?) and (position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{"testname", 17}))
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})

		query, uniqueKeyArgs, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			delete /* udup mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?) and (name = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17, "testname"}))
	}
	{
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		args := []interface{}{"first", 17}

		_, _, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNotNil(err)
	}
}

func TestBuildDMLDeleteQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	uniqueKeyColumns := NewColumnList([]string{"position"})
	{
		// test signed (expect no change)
		args := []interface{}{3, "testname", "first", -1, 23}
		query, uniqueKeyArgs, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			delete /* udup mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{-1}))
	}
	{
		// test unsigned
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		uniqueKeyColumns.SetUnsigned("position")
		query, uniqueKeyArgs, err := BuildDMLDeleteQuery(databaseName, tableName, tableColumns, uniqueKeyColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			delete /* udup mydb.tbl */
				from
					mydb.tbl
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{uint8(255)}))
	}
}

func TestBuildDMLInsertQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	args := []interface{}{3, "testname", "first", 17, 23}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			replace /* udup mydb.tbl */
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
	}
	{
		sharedColumns := NewColumnList([]string{"position", "name", "age", "id"})
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			replace /* udup mydb.tbl */
				into mydb.tbl
					(position, name, age, id)
				values
					(?, ?, ?, ?)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{17, "testname", 23, 3}))
	}
	{
		sharedColumns := NewColumnList([]string{"position", "name", "surprise", "id"})
		_, _, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNotNil(err)
	}
	{
		sharedColumns := NewColumnList([]string{})
		_, _, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNotNil(err)
	}
}

func TestBuildDMLInsertQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
	{
		// testing signed
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			replace /* udup mydb.tbl */
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", int8(-1), 23}))
	}
	{
		// testing unsigned
		args := []interface{}{3, "testname", "first", int8(-1), 23}
		sharedColumns.SetUnsigned("position")
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			replace /* udup mydb.tbl */
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", uint8(255), 23}))
	}
	{
		// testing unsigned
		args := []interface{}{3, "testname", "first", int32(-1), 23}
		sharedColumns.SetUnsigned("position")
		query, sharedArgs, err := BuildDMLInsertQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, args)
		test.S(t).ExpectNil(err)
		expected := `
			replace /* udup mydb.tbl */
				into mydb.tbl
					(id, name, position, age)
				values
					(?, ?, ?, ?)
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", uint32(4294967295), 23}))
	}
}

func TestBuildDMLUpdateQuery(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	valueArgs := []interface{}{3, "testname", "newval", 17, 23}
	whereArgs := []interface{}{3, "testname", "findme", 17, 56}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"position"})
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17}))
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"position", "name"})
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?) and (name = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{17, "testname"}))
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age"})
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{56}))
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age", "position", "id", "name"})
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((age = ?) and (position = ?) and (id = ?) and (name = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{56, 17, 3, "testname"}))
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{"age", "surprise"})
		_, _, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNotNil(err)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		uniqueKeyColumns := NewColumnList([]string{})
		_, _, _, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNotNil(err)
	}
	{
		sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
		mappedColumns := NewColumnList([]string{"id", "name", "role", "age"})
		uniqueKeyColumns := NewColumnList([]string{"id"})
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, mappedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, role=?, age=?
				where
					((id = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", 17, 23}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{3}))
	}
}

func TestBuildDMLUpdateQuerySignedUnsigned(t *testing.T) {
	databaseName := "mydb"
	tableName := "tbl"
	tableColumns := NewColumnList([]string{"id", "name", "rank", "position", "age"})
	valueArgs := []interface{}{3, "testname", "newval", int8(-17), int8(-2)}
	whereArgs := []interface{}{3, "testname", "findme", int8(-3), 56}
	sharedColumns := NewColumnList([]string{"id", "name", "position", "age"})
	uniqueKeyColumns := NewColumnList([]string{"position"})
	{
		// test signed
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", int8(-17), int8(-2)}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{int8(-3)}))
	}
	{
		// test unsigned
		sharedColumns.SetUnsigned("age")
		uniqueKeyColumns.SetUnsigned("position")
		query, sharedArgs, uniqueKeyArgs, err := BuildDMLUpdateQuery(databaseName, tableName, tableColumns, sharedColumns, sharedColumns, uniqueKeyColumns, valueArgs, whereArgs)
		test.S(t).ExpectNil(err)
		expected := `
			update /* udup mydb.tbl */
			  mydb.tbl
					set id=?, name=?, position=?, age=?
				where
					((position = ?))
		`
		test.S(t).ExpectEquals(normalizeQuery(query), normalizeQuery(expected))
		test.S(t).ExpectTrue(reflect.DeepEqual(sharedArgs, []interface{}{3, "testname", int8(-17), uint8(254)}))
		test.S(t).ExpectTrue(reflect.DeepEqual(uniqueKeyArgs, []interface{}{uint8(253)}))
	}
}
