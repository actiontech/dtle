// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"

const int sizeof_dpiData = sizeof(void);

void godror_setFromString(dpiVar *dv, uint32_t pos, const _GoString_ value) {
	uint32_t length;
	length = _GoStringLen(value);
	if( length == 0 ) {
		return;
	}
	dpiVar_setFromBytes(dv, pos, _GoStringPtr(value), length);
}
*/
import "C"
import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/godror/knownpb/timestamppb"
)

type NullTime = sql.NullTime

var nullTime interface{} = nil

type stmtOptions struct {
	boolString         boolString
	fetchArraySize     int // zero means DefaultFetchArraySize
	prefetchCount      int // zero means DefaultPrefetchCount, -1 is zero.
	arraySize          int
	callTimeout        time.Duration
	execMode           C.dpiExecMode
	plSQLArrays        bool
	lobAsReader        bool
	nullDateAsZeroTime bool
	deleteFromCache    bool
	numberAsString     bool
}

type boolString struct {
	True, False string
}

func (bs boolString) IsZero() bool { return bs.True == "" && bs.False == "" }
func (bs boolString) MaxLen() int {
	n := len(bs.True)
	if m := len(bs.False); m > n {
		return m
	}
	return n
}
func (bs boolString) ToString(b bool) string {
	if b {
		return bs.True
	}
	return bs.False
}
func (bs boolString) FromString(s string) bool {
	return s == bs.True && bs.True != "" || bs.True == "" && s != bs.False
}

func (o stmtOptions) BoolString() boolString { return o.boolString }

func (o stmtOptions) ExecMode() C.dpiExecMode {
	if o.execMode == 0 {
		return C.DPI_MODE_EXEC_DEFAULT
	}
	return o.execMode
}

func (o stmtOptions) ArraySize() int {
	if o.arraySize <= 0 {
		return DefaultArraySize
	} else if o.arraySize > 1<<16 {
		return 1 << 16
	}
	return o.arraySize
}
func (o stmtOptions) PrefetchCount() int {
	switch n := o.prefetchCount; {
	case n == 0:
		return DefaultPrefetchCount
	case n < 0:
		return 0
	default:
		return n
	}
}
func (o stmtOptions) FetchArraySize() int {
	if n := o.fetchArraySize; n <= 0 {
		return DefaultFetchArraySize
	} else {
		return n
	}
}
func (o stmtOptions) PlSQLArrays() bool { return o.plSQLArrays }

func (o stmtOptions) ClobAsString() bool { return !o.lobAsReader }
func (o stmtOptions) LobAsReader() bool  { return o.lobAsReader }
func (o stmtOptions) NullDate() interface{} {
	if o.nullDateAsZeroTime {
		return time.Time{}
	}
	return nullTime
}
func (o stmtOptions) DeleteFromCache() bool { return o.deleteFromCache }
func (o stmtOptions) NumberAsString() bool  { return o.numberAsString }

// Option holds statement options.
//
// Use it "naked", without sql.Named!
type Option func(*stmtOptions)

// BoolToString is an option that governs convertsion from bool to string in the database.
// This is for converting from bool to string, from outside of the database
// (which does not have a BOOL(EAN) column (SQL) type, only a BOOLEAN PL/SQL type).
//
// This will be used only with DML statements and when the PlSQLArrays Option is not used.
//
// For the other way around, use an sql.Scanner that converts from string to bool. For example:
//
//   type Booler bool
//   var _ sql.Scanner = Booler{}
//   func (b Booler) Scan(src interface{}) error {
//     switch src := src.(type) {
//       case int: *b = x == 1
//       case string: *b = x == "Y" || x == "T"  // or any string your database model treats as truth value
//       default: return fmt.Errorf("unknown scanner source %T", src)
//     }
//     return nil
//   }
//
// Such a type cannot be included in this package till we can inject the truth strings into the scanner method.
func BoolToString(trueVal, falseVal string) Option {
	return func(o *stmtOptions) { o.boolString = boolString{True: trueVal, False: falseVal} }
}

// PlSQLArrays is to signal that the slices given in arguments of Exec to
// be left as is - the default is to treat them as arguments for ExecMany.
//
// Use it "naked", without sql.Named!
var PlSQLArrays Option = func(o *stmtOptions) { o.plSQLArrays = true }

// FetchRowCount is DEPRECATED, use FetchArraySize.
//
// It returns an option to set the rows to be fetched, overriding DefaultFetchRowCount.
//
// Use it "naked", without sql.Named!
func FetchRowCount(rowCount int) Option { return FetchArraySize(rowCount) }

// FetchArraySize returns an option to set the rows to be fetched, overriding DefaultFetchRowCount.
//
// For choosing FetchArraySize and PrefetchCount, see https://cx-oracle.readthedocs.io/en/latest/user_guide/tuning.html#choosing-values-for-arraysize-and-prefetchrows
//
// Use it "naked", without sql.Named!
func FetchArraySize(rowCount int) Option {
	return func(o *stmtOptions) {
		if rowCount > 0 {
			o.fetchArraySize = rowCount
		} else {
			o.fetchArraySize = 0
		}
	}
}

// PrefetchCount returns an option to set the rows to be fetched, overriding DefaultPrefetchCount.
//
// For choosing FetchArraySize and PrefetchCount, see https://cx-oracle.readthedocs.io/en/latest/user_guide/tuning.html#choosing-values-for-arraysize-and-prefetchrows
//
// WARNING: If you will take a REF CURSOR, the driver will start prefetching, so if you give that cursor to a stored procedure, that won't see the prefetched rows!
//
// Use it "naked", without sql.Named!
func PrefetchCount(rowCount int) Option {
	return func(o *stmtOptions) {
		if rowCount > 0 {
			o.prefetchCount = rowCount
		} else {
			o.prefetchCount = -1
		}
	}
}

// ArraySize returns an option to set the array size to be used, overriding DefaultArraySize.
//
// Use it "naked", without sql.Named!
func ArraySize(arraySize int) Option {
	if arraySize <= 0 {
		return nil
	}
	return func(o *stmtOptions) { o.arraySize = arraySize }
}
func parseOnly(o *stmtOptions) { o.execMode = C.DPI_MODE_EXEC_PARSE_ONLY }

// ParseOnly returns an option to set the ExecMode to only Parse.
//
// Use it "naked", without sql.Named!
func ParseOnly() Option {
	return parseOnly
}

func describeOnly(o *stmtOptions) { o.execMode = C.DPI_MODE_EXEC_DESCRIBE_ONLY }

// ClobAsString returns an option to force fetching CLOB columns as strings.
//
// Deprecated: CLOBs are returned as string by default - for CLOB, use LobAsReader.
// EXCEPT for Object attributes, those are returned as-is - as lobReader.
//
// Use it "naked", without sql.Named!
func ClobAsString() Option { return func(o *stmtOptions) { o.lobAsReader = false } }

// LobAsReader is an option to set query columns of CLOB/BLOB to be returned as a Lob.
//
// LOB as a reader and writer is not the most performant at all. Yes, OCI
// and ODPI-C provide a way to retrieve this data directly. Effectively,
// all you need to do is tell ODPI-C that you want a "long string" or "long
// raw" returned. You can do that by telling ODPI-C you want a variable
// with oracleTypeNum=DPI_ORACLE_TYPE_LONG_VARCHAR or
// DPI_ORACLE_TYPE_LONG_RAW and nativeTypeNum=DPI_NATIVE_TYPE_BYTES. ODPI-C
// will handle all of the dynamic fetching and allocation that is required.
// :-) You can also use DPI_ORACLE_TYPE_VARCHAR and DPI_ORACLE_TYPE_RAW as
// long as you set the size > 32767 -- whichever way you wish to use.
//
// With the use of LOBs, there is one round-trip to get the LOB locators,
// then a round-trip for each read() that is performed. If you request the
// length there is another round-trip required. So if you fetch 100 rows
// with 2 CLOB columns, that means you get 401 round-trips. Using
// string/[]bytes directly means only one round trip. So you can see that
// if your database is remote with high latency you can have a significant
// performance penalty!
//
// EXCEPT for Object attributes, those are returned as-is - as lobReader.
//
// Use it "naked", without sql.Named!
func LobAsReader() Option { return func(o *stmtOptions) { o.lobAsReader = true } }

// CallTimeout sets the round-trip timeout (OCI_ATTR_CALL_TIMEOUT).
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/handle-and-descriptor-attributes.html#GUID-D8EE68EB-7E38-4068-B06E-DF5686379E5E
func CallTimeout(d time.Duration) Option {
	return func(o *stmtOptions) { o.callTimeout = d }
}

// NullDateAsZeroTime is an option to return NULL DATE columns as time.Time{} instead of nil.
// If you must Scan into time.Time (cannot use sql.NullTime), this may help.
func NullDateAsZeroTime() Option { return func(o *stmtOptions) { o.nullDateAsZeroTime = true } }

// DeleteFromCache is an option to delete the statement from the statement cache.
func DeleteFromCache() Option { return func(o *stmtOptions) { o.deleteFromCache = true } }

// NumberAsString is an option to return numbers a string, not Number.
func NumberAsString() Option { return func(o *stmtOptions) { o.numberAsString = true } }

const minChunkSize = 1 << 16

var _ driver.Stmt = (*statement)(nil)
var _ driver.StmtQueryContext = (*statement)(nil)
var _ driver.StmtExecContext = (*statement)(nil)
var _ driver.NamedValueChecker = (*statement)(nil)

type statement struct {
	ctx context.Context
	*conn
	dpiStmt  *C.dpiStmt
	query    string
	columns  []Column
	isSlice  []bool
	gets     []dataGetter
	dests    []interface{}
	data     [][]C.dpiData
	vars     []*C.dpiVar
	varInfos []varInfo
	stmtOptions
	arrLen      int
	dpiStmtInfo C.dpiStmtInfo
	sync.Mutex
}
type dataGetter func(v interface{}, data []C.dpiData) error

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (st *statement) Close() error {
	if st == nil {
		return nil
	}
	st.Lock()
	defer st.Unlock()

	return st.closeNotLocking()
}
func (st *statement) closeNotLocking() error {
	if st == nil || st.dpiStmt == nil {
		return nil
	}

	c, dpiStmt, vars := st.conn, st.dpiStmt, st.vars
	st.vars = nil
	st.isSlice = nil
	st.query = ""
	st.data = nil
	st.varInfos = nil
	st.gets = nil
	st.dests = nil
	st.columns = nil
	st.dpiStmt = nil
	st.conn = nil
	st.dpiStmtInfo = C.dpiStmtInfo{}
	st.ctx = nil

	if logger := getLogger(); logger != nil {
		logger.Log("msg", "statement.closeNotLocking", "st", fmt.Sprintf("%p", st), "refCount", dpiStmt.refCount)
		if false {
			var a [4096]byte
			stack := a[:runtime.Stack(a[:], false)]
			logger.Log("msg", "closeNotLocking", "stack", string(stack))
		}
	}
	for _, v := range vars[:cap(vars)] {
		if v != nil {
			C.dpiVar_release(v)
		}
	}
	if dpiStmt.refCount > 0 {
		C.dpiStmt_release(dpiStmt)
	}
	if c == nil {
		return driver.ErrBadConn
	}
	return nil
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (st *statement) Exec(args []driver.Value) (driver.Result, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i].Ordinal = i + 1
		nargs[i].Value = arg
	}
	return st.ExecContext(context.Background(), nargs)
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (st *statement) Query(args []driver.Value) (driver.Rows, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i].Ordinal = i + 1
		nargs[i].Value = arg
	}
	return st.QueryContext(context.Background(), nargs)
}

func newDoneCh() (<-chan struct{}, func()) {
	done := make(chan struct{})
	var once sync.Once
	return done, func() { once.Do(func() { close(done) }) }
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
//
// Cancelation/timeout is honored, execution is broken, but you may have to disable out-of-bound execution - see https://github.com/oracle/odpi/issues/116 for details.
func (st *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logger := ctxGetLog(ctx)
	if logger != nil {
		logger.Log("msg", "ExecContext", "stmt", fmt.Sprintf("%p", st), "args", args)
	}

	st.Lock()
	defer st.Unlock()
	if st.conn == nil {
		return nil, driver.ErrBadConn
	}
	st.ctx = ctx

	if st.dpiStmt == nil && st.query == getConnection {
		*(args[0].Value.(sql.Out).Dest.(*interface{})) = st.conn
		return driver.ResultNoRows, nil
	}

	st.conn.mu.RLock()
	defer st.conn.mu.RUnlock()

	// HandleDeadline for all ODPI calls called below
	done, closeDone := newDoneCh()
	defer closeDone()
	if err := st.handleDeadline(ctx, done); err != nil {
		return nil, err
	}
	var err error
	closeIfBadConn := func(err error) error {
		closeDone()
		if err == nil {
			return nil
		}
		c := st.conn
		st.closeNotLocking()
		return maybeBadConn(err, c)
	}

	// bind variables
	if err = st.bindVars(args, logger); err != nil {
		return nil, closeIfBadConn(err)
	}

	mode := st.ExecMode()
	//fmt.Printf("%p.%p: inTran? %t\n%s\n", st.conn, st, st.inTransaction, st.query)
	if !st.inTransaction {
		mode |= C.DPI_MODE_EXEC_COMMIT_ON_SUCCESS
	}
	if st.DeleteFromCache() {
		C.dpiStmt_deleteFromCache(st.dpiStmt)
	}
	// execute
	var f func() C.int
	many := !st.PlSQLArrays() && st.arrLen > 0
	if many {
		f = func() C.int { return C.dpiStmt_executeMany(st.dpiStmt, mode, C.uint32_t(st.arrLen)) }
	} else {
		f = func() C.int { return C.dpiStmt_execute(st.dpiStmt, mode, nil) }
	}
	for i := 0; i < 3; i++ {
		if logger != nil {
			logger.Log("C", "dpiStmt_execute", "st", fmt.Sprintf("%p", st.dpiStmt), "many", many, "mode", mode, "len", st.arrLen)
		}
		if err = st.checkExec(f); err == nil {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !isInvalidErr(err) {
			break
		}
	}
	if err != nil {
		return nil, closeIfBadConn(err) //fmt.Errorf("dpiStmt_execute(mode=%d arrLen=%d): %w", mode, arrLen, err))
	}

	if logger != nil {
		logger.Log("gets", st.gets, "dests", st.dests)
	}
	for i, get := range st.gets {
		if get == nil {
			continue
		}
		i := i
		if st.dpiStmtInfo.isReturning == 1 {
			var n C.uint32_t
			data := &st.data[i][0]
			if err := st.checkExec(func() C.int { return C.dpiVar_getReturnedData(st.vars[i], 0, &n, &data) }); err != nil {
				return nil, closeIfBadConn(fmt.Errorf("%d.getReturnedData: %w", i, err))
			}
			if n == 0 {
				st.data[i] = st.data[i][:0]
			} else {
				//st.data[i] = unsafe.Slice(data, n) // go1.17
				st.data[i] = dpiDataSlice(data, n)
			}
		}
		dest := st.dests[i]
		if !st.isSlice[i] {
			if err := get(dest, st.data[i]); err != nil {
				if logger != nil {
					logger.Log("get", i, "error", err)
				}
				return nil, closeIfBadConn(fmt.Errorf("%d. get[%d]: %w", i, 0, err))
			}
			if logger != nil {
				logger.Log("msg", "get-not-slice", "i", i, "dest", dest)
			}
			continue
		}
		var n C.uint32_t = 1
		if err := st.checkExec(func() C.int { return C.dpiVar_getNumElementsInArray(st.vars[i], &n) }); err != nil {
			if logger != nil {
				logger.Log("msg", "getNumElementsInArray", "i", i, "error", err)
			}
			return nil, closeIfBadConn(fmt.Errorf("%d.getNumElementsInArray: %w", i, err))
		}
		//fmt.Printf("i=%d dest=%T %#v\n", i, dest, dest)
		if err := get(dest, st.data[i][:n]); err != nil {
			if logger != nil {
				logger.Log("msg", "get", "i", i, "n", n, "error", err)
			}
			return nil, closeIfBadConn(fmt.Errorf("%d. get: %w", i, err))
		}
		if false && logger != nil {
			logger.Log("msg", "get-slice", "i", i, "dest", dest)
		}
	}
	var count C.uint64_t
	if st.checkExec(func() C.int { return C.dpiStmt_getRowCount(st.dpiStmt, &count) }) != nil {
		return nil, nil
	}
	return driver.RowsAffected(count), nil
}

// QueryContext executes a query that may return rows, such as a SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
//
// Cancelation/timeout is honored, execution is broken, but you may have to disable out-of-bound execution - see https://github.com/oracle/odpi/issues/116 for details.
func (st *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	st.Lock()
	defer st.Unlock()
	if st.conn == nil {
		return nil, driver.ErrBadConn
	}
	st.conn.mu.RLock()
	defer st.conn.mu.RUnlock()
	return st.queryContextNotLocked(ctx, args)
}

func (st *statement) queryContextNotLocked(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st.ctx = ctx

	logger := ctxGetLog(ctx)
	switch st.query {
	case getConnection:
		if logger != nil {
			logger.Log("msg", "QueryContext", "args", args)
		}
		return &directRow{conn: st.conn, query: st.query, result: []interface{}{st.conn}}, nil

	case wrapResultset:
		if logger != nil {
			logger.Log("msg", "QueryContext", "args", args)
		}
		return args[0].Value.(driver.Rows), nil
	}

	done, closeDone := newDoneCh()
	defer closeDone()
	if err := st.handleDeadline(ctx, done); err != nil {
		return nil, err
	}
	var err error
	closeIfBadConn := func(err error) error {
		closeDone()
		if err == nil {
			return nil
		}
		c := st.conn
		st.closeNotLocking()
		return maybeBadConn(err, c)
	}
	// HandleDeadline for all ODPI calls called below

	//fmt.Printf("QueryContext(%+v)\n", args)
	// bind variables
	if err = st.bindVars(args, logger); err != nil {
		return nil, closeIfBadConn(err)
	}

	mode := st.ExecMode()
	//fmt.Printf("%p.%p: inTran? %t\n%s\n", st.conn, st, st.inTransaction, st.query)
	if !st.inTransaction {
		mode |= C.DPI_MODE_EXEC_COMMIT_ON_SUCCESS
	}
	// set Prefetch Parameters before execute
	C.dpiStmt_setFetchArraySize(st.dpiStmt, C.uint32_t(st.FetchArraySize()))
	C.dpiStmt_setPrefetchRows(st.dpiStmt, C.uint32_t(st.PrefetchCount()))

	// execute
	var colCount C.uint32_t
	f := func() C.int { return C.dpiStmt_execute(st.dpiStmt, mode, &colCount) }
	for i := 0; i < 3; i++ {
		if err = st.checkExec(f); err == nil {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !isInvalidErr(err) {
			break
		}
	}
	if err != nil {
		return nil, closeIfBadConn(fmt.Errorf("dpiStmt_execute: %w", err))
	}

	rows, err := st.openRows(int(colCount))
	return rows, closeIfBadConn(err)
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (st *statement) NumInput() int {
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "NumInput", "stmt", fmt.Sprintf("%p", st), "dpiStmt", fmt.Sprintf("%p", st.dpiStmt), "query", st.query)
	}
	if st.query == wrapResultset {
		return 1
	}
	if st.dpiStmt == nil {
		switch st.query {
		case getConnection, wrapResultset:
			return 1
		}
		return 0
	}

	st.Lock()
	defer st.Unlock()
	var cnt C.uint32_t
	if err := st.checkExec(func() C.int { return C.dpiStmt_getBindCount(st.dpiStmt, &cnt) }); err != nil {
		if logger != nil {
			logger.Log("msg", "getBindCount", "error", err)
		}
		if st.conn == nil {
			panic(driver.ErrBadConn)
		}
		panic(err)
	}
	if cnt < 2 { // 1 can't decrease...
		if logger != nil {
			logger.Log("msg", "NumInput", "count", cnt, "stmt", fmt.Sprintf("%p", st))
		}
		return int(cnt)
	}

	var prevColon bool
	var mayHaveBoundName bool
	for _, r := range st.query {
		if r == ':' {
			prevColon = true
			continue
		}
		if prevColon {
			prevColon = false
			if 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' {
				mayHaveBoundName = true
				break
			}
		}
	}
	if !mayHaveBoundName {
		return int(cnt)
	}

	names := make([]*C.char, int(cnt))
	lengths := make([]C.uint32_t, int(cnt))
	if err := st.checkExec(func() C.int { return C.dpiStmt_getBindNames(st.dpiStmt, &cnt, &names[0], &lengths[0]) }); err != nil {
		if logger != nil {
			logger.Log("msg", "getBindNames", "error", err)
		}
		if st.conn == nil {
			panic(driver.ErrBadConn)
		}
		panic(err)
	}
	if logger != nil {
		logger.Log("msg", "NumInput", "count", cnt, "stmt", fmt.Sprintf("%p", st))
	}

	// return the number of *unique* arguments
	return int(cnt)
}

type argInfo struct {
	objType     *C.dpiObjectType
	set         dataSetter
	bufSize     int
	typ         C.dpiOracleTypeNum
	natTyp      C.dpiNativeTypeNum
	isIn, isOut bool
}

// bindVars binds the given args into new variables.
func (st *statement) bindVars(args []driver.NamedValue, logger Logger) error {
	if logger != nil {
		logger.Log("enter", "bindVars", "st", fmt.Sprintf("%p", st), "args", args)
	}
	var named bool
	if cap(st.vars) < len(args) {
		st.vars = make([]*C.dpiVar, len(args))
	} else {
		st.vars = st.vars[:len(args)]
	}
	if cap(st.varInfos) < len(args) {
		st.varInfos = make([]varInfo, len(args))
	} else {
		st.varInfos = st.varInfos[:len(args)]
	}
	if cap(st.data) < len(args) {
		st.data = make([][]C.dpiData, len(args))
	} else {
		st.data = st.data[:len(args)]
	}
	if cap(st.gets) < len(args) {
		st.gets = make([]dataGetter, len(args))
	} else {
		st.gets = st.gets[:len(args)]
	}
	if cap(st.dests) < len(args) {
		st.dests = make([]interface{}, len(args))
	} else {
		st.dests = st.dests[:len(args)]
	}
	if cap(st.isSlice) < len(args) {
		st.isSlice = make([]bool, len(args))
	} else {
		st.isSlice = st.isSlice[:len(args)]
	}

	rArgs := make([]reflect.Value, len(args))
	minArrLen, maxArrLen := -1, -1

	st.arrLen = minArrLen
	maxArraySize := st.ArraySize()

	infos := make([]argInfo, len(args))
	//fmt.Printf("bindVars %d\n", len(args))
	for i, a := range args {
		st.gets[i] = nil
		st.dests[i] = nil
		if !named {
			named = a.Name != ""
		}
		info := &(infos[i])
		info.isIn = true
		value := a.Value
		if out, ok := value.(sql.Out); ok {
			if !st.PlSQLArrays() && st.arrLen > 1 {
				st.arrLen = maxArraySize
			}
			info.isIn, info.isOut = out.In, true
			value = out.Dest
		}
		st.dests[i] = value
		rv := reflect.ValueOf(value)
		if info.isOut {
			if false && rv.IsNil() {
				fmt.Printf("%d. v=%T %#v kind=%s\n", i, value, value, reflect.ValueOf(value).Kind())
			}
			if rv.Kind() == reflect.Ptr {
				rv = rv.Elem()
				value = rv.Interface()
			}
		}
		st.isSlice[i] = false
		rArgs[i] = rv
		if rv.Kind() == reflect.Ptr {
			// deref in rArgs, but NOT value!
			rArgs[i] = rv.Elem()
		}
		if _, isByteSlice := value.([]byte); !isByteSlice {
			st.isSlice[i] = rArgs[i].Kind() == reflect.Slice
			if !st.PlSQLArrays() && st.isSlice[i] {
				n := rArgs[i].Len()
				if minArrLen == -1 || n < minArrLen {
					minArrLen = n
				}
				if maxArrLen == -1 || n > maxArrLen {
					maxArrLen = n
				}
			}
		}

		if logger != nil {
			logger.Log("msg", "bindVars", "i", i, "in", info.isIn, "out", info.isOut, "value", fmt.Sprintf("%[1]p %[1]T %#[1]v", st.dests[i]))
		}
	}

	if maxArrLen > maxArraySize {
		if st.arrLen == maxArraySize {
			st.arrLen = maxArrLen
		}
		maxArraySize = maxArrLen
	}
	doManyCount := 1
	doExecMany := !st.PlSQLArrays()
	if doExecMany {
		if minArrLen != -1 && minArrLen != maxArrLen {
			return fmt.Errorf("PlSQLArrays is not set, but has different lengthed slices (min=%d < %d=max)", minArrLen, maxArrLen)
		}
		st.arrLen = minArrLen
		if doExecMany = st.arrLen > 1; doExecMany {
			doManyCount = st.arrLen
		}
	}
	if logger != nil {
		logger.Log("doManyCount", doManyCount, "arrLen", st.arrLen, "doExecMany", doExecMany, "minArrLen", "maxArrLen")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for i := range args {
		info := &(infos[i])
		value := st.dests[i]

		var err error
		if value, err = st.bindVarTypeSwitch(info, &(st.gets[i]), value); err != nil {
			return fmt.Errorf("%d. arg: %w", i+1, err)
		}

		var rv reflect.Value
		if st.isSlice[i] {
			rv = reflect.ValueOf(value)
		}

		n := doManyCount
		if st.PlSQLArrays() && st.isSlice[i] {
			n = rv.Len()
			if info.isOut {
				n = rv.Cap()
			}
		}
		if logger != nil {
			logger.Log("msg", "newVar", "i", i, "plSQLArrays", st.PlSQLArrays(), "typ", int(info.typ), "natTyp", int(info.natTyp), "sliceLen", n, "bufSize", info.bufSize, "isSlice", st.isSlice[i])
		}
		//i, st.PlSQLArrays(), info.typ, info.natTyp dataSliceLen, info.bufSize)
		vi := varInfo{
			IsPLSArray: st.PlSQLArrays() && st.isSlice[i],
			Typ:        info.typ, NatTyp: info.natTyp,
			SliceLen: n, BufSize: info.bufSize,
			ObjectType: info.objType,
		}
		if vi.IsPLSArray && vi.SliceLen > maxArraySize {
			return fmt.Errorf("maximum array size allowed is %d", maxArraySize)
		}
		mustAllocate := st.vars[i] == nil || st.data[i] == nil
		if !mustAllocate && st.varInfos[i] != vi {
			C.dpiVar_release(st.vars[i])
			mustAllocate = true
		}
		if mustAllocate {
			if st.vars[i], st.data[i], err = st.newVar(vi); err != nil {
				return fmt.Errorf("%d: %w", i, err)
			}
			st.varInfos[i] = vi
		}

		// Have to setNumElementsInArray for the actual lengths for PL/SQL arrays
		dv, data := st.vars[i], st.data[i]
		if !info.isIn {
			if st.PlSQLArrays() {
				if logger != nil {
					logger.Log("C", "dpiVar_setNumElementsInArray", "i", i, "n", 0)
				}
				if err := st.checkExecNoLOT(func() C.int { return C.dpiVar_setNumElementsInArray(dv, C.uint32_t(0)) }); err != nil {
					return fmt.Errorf("setNumElementsInArray[%d](%d): %w", i, 0, err)
				}
			}
			continue
		}

		if !st.isSlice[i] {
			if logger != nil {
				logger.Log("msg", "set", "i", i, "value", fmt.Sprintf("%T=%#v", value, value))
			}
			if err := info.set(dv, data[:1], value); err != nil {
				return fmt.Errorf("set(data[%d][%d], %#v (%T)): %w", i, 0, value, value, err)
			}
			continue
		}

		if st.PlSQLArrays() {
			n = rv.Len()

			if logger != nil {
				logger.Log("C", "dpiVar_setNumElementsInArray", "i", i, "n", n)
			}
			if err := st.checkExecNoLOT(func() C.int { return C.dpiVar_setNumElementsInArray(dv, C.uint32_t(n)) }); err != nil {
				return fmt.Errorf("%+v.setNumElementsInArray[%d](%d): %w", dv, i, n, err)
			}
		}
		//fmt.Println("n:", len(st.data[i]))
		if err := info.set(dv, data, value); err != nil {
			return err
		}
	}

	if !named {
		for i, v := range st.vars {
			i, v := i, v
			if err := st.checkExecNoLOT(func() C.int { return C.dpiStmt_bindByPos(st.dpiStmt, C.uint32_t(i+1), v) }); err != nil {
				return fmt.Errorf("bindByPos[%d]: %w", i, err)
			}
		}
		return nil
	}
	for i, a := range args {
		name := a.Name
		if name == "" {
			name = strconv.Itoa(a.Ordinal)
		}
		//fmt.Printf("bindByName(%q)\n", name)
		cName := C.CString(name)
		err := st.checkExecNoLOT(func() C.int { return C.dpiStmt_bindByName(st.dpiStmt, cName, C.uint32_t(len(name)), st.vars[i]) })
		C.free(unsafe.Pointer(cName))
		if err != nil {
			return fmt.Errorf("bindByName[%q]: %w", name, err)
		}
	}
	return nil
}

func (st *statement) bindVarTypeSwitch(info *argInfo, get *dataGetter, value interface{}) (interface{}, error) {
	nilPtr := false
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "bindVarTypeSwitch", "info", info, "value", fmt.Sprintf("[%T]%v", value, value))
	}
	vlr, isValuer := value.(driver.Valuer)

	switch value.(type) {
	case *driver.Rows, *Object, *timestamppb.Timestamp:
	default:
		if rv := reflect.ValueOf(value); rv.Kind() == reflect.Ptr {
			if nilPtr = rv.IsNil(); nilPtr {
				info.set = dataSetNull
				value = reflect.Zero(rv.Type().Elem()).Interface()
			} else {
				value = rv.Elem().Interface()
				if !isValuer {
					vlr, isValuer = value.(driver.Valuer)
				}
			}
		}
	}

	switch v := value.(type) {
	case Lob, []Lob:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_BLOB, C.DPI_NATIVE_TYPE_LOB
		var isClob bool
		switch v := v.(type) {
		case Lob:
			isClob = v.IsClob
		case []Lob:
			isClob = len(v) > 0 && v[0].IsClob
		}
		if isClob {
			info.typ = C.DPI_ORACLE_TYPE_CLOB
		}
		info.set = st.dataSetLOB
		if info.isOut {
			*get = st.dataGetLOB
		}
	case *driver.Rows:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_STMT, C.DPI_NATIVE_TYPE_STMT
		info.set = dataSetNull
		if info.isOut {
			*get = st.dataGetStmt
		}
	case int, []int:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_INT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case int8, []int8:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_NATIVE_TYPE_INT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case int16, []int16:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_NATIVE_TYPE_INT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case int32, []int32:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_NATIVE_TYPE_INT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case int64, []int64, sql.NullInt64, []sql.NullInt64:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_INT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case uint, []uint:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_UINT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case uint8:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_UINT, C.DPI_NATIVE_TYPE_UINT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case uint16, []uint16:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_UINT, C.DPI_NATIVE_TYPE_UINT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case uint32, []uint32:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_UINT, C.DPI_NATIVE_TYPE_UINT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case uint64, []uint64:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_UINT64
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case float32, []float32:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_FLOAT, C.DPI_NATIVE_TYPE_FLOAT
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case float64, []float64:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_DOUBLE, C.DPI_NATIVE_TYPE_DOUBLE
		if !nilPtr {
			info.set = dataSetNumber
			if info.isOut {
				*get = dataGetNumber
			}
		}
	case sql.NullFloat64, []sql.NullFloat64:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_DOUBLE, C.DPI_NATIVE_TYPE_DOUBLE
		info.set = dataSetNumber
		if info.isOut {
			*get = dataGetNumber
		}
	case bool, []bool:
		if st.dpiStmtInfo.isPLSQL == 1 || st.stmtOptions.boolString.IsZero() || st.PlSQLArrays() {
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_BOOLEAN, C.DPI_NATIVE_TYPE_BOOLEAN
			info.set = dataSetBool
			if info.isOut {
				*get = dataGetBool
			}
		} else {
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_NATIVE_TYPE_BYTES
			info.bufSize = st.stmtOptions.boolString.MaxLen()
			info.set = st.dataSetBoolBytes
			if info.isOut {
				*get = st.dataGetBoolBytes
			}
		}

	case []byte, [][]byte:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_RAW, C.DPI_NATIVE_TYPE_BYTES
		info.set = dataSetBytes
		if info.isOut {
			info.bufSize = 32767
			*get = dataGetBytes
		} else {
			switch v := v.(type) {
			case []byte:
				info.bufSize = len(v)
			case [][]byte:
				for _, b := range v {
					if n := len(b); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
		}

	case Number, []Number:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_BYTES
		info.set = dataSetBytes
		if info.isOut {
			info.bufSize = 32767
			*get = dataGetBytes
		} else {
			switch v := v.(type) {
			case Number:
				info.bufSize = len(v)
			case []Number:
				for _, s := range v {
					if n := len(s); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
		}

	case string, []string, nil:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_NATIVE_TYPE_BYTES
		info.set = dataSetBytes
		if info.isOut {
			info.bufSize = 32767
			*get = dataGetBytes
		} else {
			switch v := v.(type) {
			case string:
				info.bufSize = 4 * len(v)
			case []string:
				for _, s := range v {
					if n := 4 * len(s); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
		}

	case time.Time, NullTime, *timestamppb.Timestamp:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_TIMESTAMP_TZ, C.DPI_NATIVE_TYPE_TIMESTAMP
		info.set = st.conn.dataSetTime
		if info.isOut {
			*get = st.conn.dataGetTime
		}

	case []time.Time, []NullTime, []*timestamppb.Timestamp:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_TIMESTAMP_TZ, C.DPI_NATIVE_TYPE_TIMESTAMP
		if st.plSQLArrays {
			info.typ = C.DPI_ORACLE_TYPE_DATE
		}
		info.set = st.conn.dataSetTime
		if info.isOut {
			*get = st.conn.dataGetTime
		}

	case time.Duration, []time.Duration:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_INTERVAL_DS, C.DPI_NATIVE_TYPE_INTERVAL_DS
		info.set = st.conn.dataSetIntervalDS
		if info.isOut {
			*get = st.conn.dataGetIntervalDS
		}

	case Object:
		info.objType = v.ObjectType.dpiObjectType
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_OBJECT, C.DPI_NATIVE_TYPE_OBJECT
		info.set = st.dataSetObject
		if info.isOut {
			*get = st.dataGetObject
		}

	case *Object:
		if !nilPtr && v != nil {
			info.objType = v.ObjectType.dpiObjectType
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_OBJECT, C.DPI_NATIVE_TYPE_OBJECT
		}
		info.set = st.dataSetObject
		if info.isOut {
			*get = st.dataGetObject
		}

	case userType:
		info.objType = v.ObjectRef().ObjectType.dpiObjectType
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_OBJECT, C.DPI_NATIVE_TYPE_OBJECT
		info.set = st.dataSetObject
		if info.isOut {
			*get = st.dataGetObject
		}
	case JSON:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_JSON, C.DPI_NATIVE_TYPE_JSON
		info.set = st.conn.dataSetJSON
		if info.isOut {
			*get = st.conn.dataGetJSON
		}
	case JSONString, []JSONString:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_JSON, C.DPI_NATIVE_TYPE_JSON
		info.set = st.dataSetJSONString
		if info.isOut {
			*get = st.dataGetJSONString
		}
	case JSONValue, []JSONValue:
		info.typ, info.natTyp = C.DPI_ORACLE_TYPE_JSON, C.DPI_NATIVE_TYPE_JSON
		info.set = st.conn.dataSetJSONValue
		if info.isOut {
			*get = st.conn.dataGetJSONValue
		}

	default:
		if logger != nil {
			logger.Log("msg", "bindVarTypeSwitch default", "value", value)
		}
		if !isValuer {
			return value, fmt.Errorf("unknown type %T", value)
		}
		oval := value
		var err error
		if value, err = vlr.Value(); err != nil {
			return value, fmt.Errorf("arg.Value(): %w", err)
		}
		if logger != nil {
			logger.Log("msg", "valuer", "old", fmt.Sprintf("[%T]%#v.Value()", oval, oval), "new", fmt.Sprintf("[%T]%#v", value, value))
		}
		return st.bindVarTypeSwitch(info, get, value)
	}

	return value, nil
}

type dataSetter func(dv *C.dpiVar, data []C.dpiData, vv interface{}) error

func dataSetNull(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	for i := range data {
		data[i].isNull = 1
	}
	return nil
}
func dataGetBool(v interface{}, data []C.dpiData) error {
	if b, ok := v.(*bool); ok {
		if len(data) == 0 || data[0].isNull == 1 {
			*b = false
			return nil
		}
		//*b = C.dpiData_getBool(&data[0]) == 1
		*b = *((*C.int)(unsafe.Pointer(&data[0].value))) == 1
		return nil
	}
	slice := v.(*[]bool)
	if cap(*slice) >= len(data) {
		*slice = (*slice)[:len(data)]
	} else {
		*slice = make([]bool, len(data))
	}
	for i := range data {
		if data[i].isNull == 1 {
			(*slice)[i] = false
			continue
		}
		//(*slice)[i] = C.dpiData_getBool(&data[i]) == 1
		(*slice)[i] = *((*C.int)(unsafe.Pointer(&data[i].value))) == 1
	}
	return nil
}
func dataSetBool(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	b := C.int(0)
	if v, ok := vv.(bool); ok {
		if v {
			b = 1
		}
		C.dpiData_setBool(&data[0], b)
		return nil
	}
	if bb, ok := vv.([]bool); ok {
		for i, v := range bb {
			if v {
				b = 1
			}
			C.dpiData_setBool(&data[i], b)
		}
		return nil
	}
	for i := range data {
		data[i].isNull = 1
	}
	return nil
}

var _ = sql.Scanner((*NullTime)(nil))

func (c *conn) dataGetTime(v interface{}, data []C.dpiData) error {
	switch x := v.(type) {
	case *time.Time:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = time.Time{}
			return nil
		}
		c.dataGetTimeC(x, &data[0])
	case *timestamppb.Timestamp:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Reset()
			return nil
		}
		var t time.Time
		c.dataGetTimeC(&t, &data[0])
		if t.IsZero() {
			x.Reset()
		} else {
			*x = *timestamppb.New(t)
		}

	case *NullTime:
		if x.Valid = !(len(data) == 0 || data[0].isNull == 1); x.Valid {
			c.dataGetTimeC(&x.Time, &data[0])
		}

	case *[]time.Time:
		n := len(data)
		if cap(*x) >= n {
			*x = (*x)[:n]
		} else {
			*x = make([]time.Time, n)
		}
		for i := range data {
			c.dataGetTimeC(&((*x)[i]), &data[i])
		}
	case *[]*timestamppb.Timestamp:
		n := len(data)
		if cap(*x) >= n {
			*x = (*x)[:n]
		} else {
			*x = make([]*timestamppb.Timestamp, n)
		}
		for i := range data {
			var t time.Time
			c.dataGetTimeC(&t, &data[i])
			if t.IsZero() {
				(*x)[i].Reset()
			} else {
				if (*x)[i] == nil {
					(*x)[i] = timestamppb.New(t)
				} else {
					*((*x)[i]) = *timestamppb.New(t)
				}
			}
		}
	case *[]NullTime:
		n := len(data)
		if cap(*x) >= n {
			*x = (*x)[:n]
		} else {
			*x = make([]NullTime, n)
		}
		for i := range data {
			if (*x)[i].Valid = !(data[i].isNull == 1); (*x)[i].Valid {
				c.dataGetTimeC(&((*x)[i].Time), &data[i])
			}
		}

	default:
		return fmt.Errorf("dataGetTime(%T): %w", v, errUnknownType)
	}
	return nil
}

var errUnknownType = errors.New("unknown type")

func (c *conn) dataGetTimeC(t *time.Time, data *C.dpiData) {
	if data.isNull == 1 {
		*t = time.Time{}
		return
	}
	//ts := C.dpiData_getTimestamp(data)
	ts := *((*C.dpiTimestamp)(unsafe.Pointer(&data.value)))
	*t = time.Date(
		int(ts.year), time.Month(ts.month), int(ts.day),
		int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
		timeZoneFor(ts.tzHourOffset, ts.tzMinuteOffset, c.Timezone()),
	)
}

var date8192begin, date8192end = time.Date(0, time.December, 31, 0, 0, 0, 0, time.UTC), time.Date(1, time.January, 2, 0, 0, 0, 0, time.UTC)

func (c *conn) dataSetTime(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	var logger Logger
	times := []time.Time{{}}
	switch x := vv.(type) {
	case time.Time:
		times[0] = x
		data[0].isNull = C.int(b2i(x.IsZero()))
	case NullTime:
		if data[0].isNull = C.int(b2i(!x.Valid)); x.Valid {
			times[0] = x.Time
		}
	case *timestamppb.Timestamp:
		times[0] = x.AsTime()
		data[0].isNull = C.int(b2i(times[0].IsZero()))

	case []time.Time:
		times = x
		for i, t := range times {
			data[i].isNull = C.int(b2i(t.IsZero()))
		}
	case []NullTime:
		if cap(times) < len(x) {
			times = make([]time.Time, len(x))
		} else {
			times = times[:len(x)]
		}
		for i, n := range x {
			if data[i].isNull = C.int(b2i(!n.Valid)); n.Valid {
				times[i] = x[i].Time
			}
		}
	case []*timestamppb.Timestamp:
		if cap(times) < len(x) {
			times = make([]time.Time, len(x))
		} else {
			times = times[:len(x)]
		}
		for i, n := range x {
			times[i] = n.AsTime()
			data[i].isNull = C.int(b2i(times[i].IsZero()))
		}

	default:
		return fmt.Errorf("dataSetTime(%T): %w", vv, errUnknownType)
	}

	//tzHour, tzMin := C.int8_t(c.tzOffSecs/3600), C.int8_t((c.tzOffSecs%3600)/60)
	if logger == nil {
		logger = getLogger()
	}
	tz := c.Timezone()
	for i, t := range times {
		if data[i].isNull == 1 {
			continue
		}
		tz, tzOff := tz, 0
		if tz != time.UTC && // Against ORA-08192
			date8192begin.Before(t) && date8192end.After(t) {
			tz = time.UTC
		}
		if t.Location() != tz {
			t = t.In(tz)
		}
		if tz != time.UTC {
			_, tzOff = t.Zone()
		}
		Y, M, D := t.Date()
		if Y <= 0 { // Oracle skips year 0, 0001-01-01 follows -0001-12-31 !
			Y--
		}
		if -4713 > Y || Y == 0 || 9999 < Y { // Against ORA-01841
			return fmt.Errorf("%v: %w", t, ErrBadDate)
		}
		h, m, s := t.Clock()
		if logger != nil {
			logger.Log("msg", "setTimestamp", "time", t.Format(time.RFC3339), "utc", t.UTC(), "tz", tzOff,
				"Y", Y, "M", M, "D", D, "h", h, "m", m, "s", s, "t", t.Nanosecond(), "tzHour", tzOff/3600, "tzMin", (tzOff%3600)/60)
		}
		C.dpiData_setTimestamp(&data[i],
			C.int16_t(Y), C.uint8_t(M), C.uint8_t(D),
			C.uint8_t(h), C.uint8_t(m), C.uint8_t(s), C.uint32_t(t.Nanosecond()),
			C.int8_t(tzOff/3600), C.int8_t((tzOff%3600)/60),
		)
	}
	return nil
}

func (c *conn) dataGetIntervalDS(v interface{}, data []C.dpiData) error {
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "dataGetIntervalDS", "data", data, "v", v)
	}
	switch x := v.(type) {
	case *time.Duration:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
			return nil
		}
		dataGetIntervalDS(x, &data[0])

	case *[]time.Duration:
		n := len(data)
		if cap(*x) >= n {
			*x = (*x)[:n]
		} else {
			*x = make([]time.Duration, n)
		}
		for i := range data {
			dataGetIntervalDS(&((*x)[i]), &data[i])
		}
	}
	return nil
}

func dataGetIntervalDS(t *time.Duration, d *C.dpiData) {
	//ds := C.dpiData_getIntervalDS(d)
	ds := *((*C.dpiIntervalDS)(unsafe.Pointer(&d.value)))
	*t = time.Duration(ds.days)*24*time.Hour +
		time.Duration(ds.hours)*time.Hour +
		time.Duration(ds.minutes)*time.Minute +
		time.Duration(ds.seconds)*time.Second +
		time.Duration(ds.fseconds)
	if logger := getLogger(); logger != nil {
		logger.Log("msg", "dataGetIntervalDS", "d", *d, "t", *t)
	}
}

func (c *conn) dataSetIntervalDS(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	times := []time.Duration{0}
	switch x := vv.(type) {
	case time.Duration:
		times[0] = x
		data[0].isNull = C.int(b2i(x == 0))

	case []time.Duration:
		times = x
		for i, t := range times {
			data[i].isNull = C.int(b2i(t == 0))
		}

	default:
		for i := range data {
			data[i].isNull = 1
		}
		return nil
	}
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "dataSetIntervalDS", "data", data, "times", times)
	}

	for i, t := range times {
		if data[i].isNull == 1 {
			continue
		}
		rem := t % (24 * time.Hour)
		d := C.int32_t(t / (24 * time.Hour))
		t, rem = rem, t%(time.Hour)
		h := C.int32_t(t / time.Hour)
		t, rem = rem, t%(time.Minute)
		m := C.int32_t(t / time.Minute)
		t, rem = rem, t%time.Second
		s := C.int32_t(t / time.Second)
		fs := C.int32_t(rem)
		if logger != nil {
			logger.Log("i", i, "t", t, "day", d, "hour", h, "minute", m, "second", s, "fsecond", fs)
		}
		C.dpiData_setIntervalDS(&data[i], d, h, m, s, fs)
		if logger != nil {
			logger.Log("i", i, "t", t, "data", data[i])
		}
	}
	return nil
}

func dataGetNumber(v interface{}, data []C.dpiData) error {
	switch x := v.(type) {
	case *int:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = int(C.dpiData_getInt64(&data[0]))
			*x = int(*((*int64)(unsafe.Pointer(&data[0].value))))
		}
	case *[]int:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, int(C.dpiData_getInt64(&data[i])))
				*x = append(*x, int(*((*int64)(unsafe.Pointer(&data[i].value)))))
			}
		}
	case *int8:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = int8(C.dpiData_getInt64(&data[0]))
			*x = *((*int8)(unsafe.Pointer(&data[0].value)))
		}
	case *[]int8:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, int8(C.dpiData_getInt64(&data[i])))
				*x = append(*x, *((*int8)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *int16:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = int16(C.dpiData_getInt64(&data[0]))
			*x = *((*int16)(unsafe.Pointer(&data[0].value)))
		}
	case *[]int16:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, int16(C.dpiData_getInt64(&data[i])))
				*x = append(*x, *((*int16)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *int32:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = int32(C.dpiData_getInt64(&data[0]))
			*x = *((*int32)(unsafe.Pointer(&data[0].value)))
		}
	case *[]int32:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, int32(C.dpiData_getInt64(&data[i])))
				*x = append(*x, *((*int32)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *int64:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = int64(C.dpiData_getInt64(&data[0]))
			*x = *((*int64)(unsafe.Pointer(&data[0].value)))
		}
	case *[]int64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, int64(C.dpiData_getInt64(&data[i])))
				*x = append(*x, *((*int64)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *sql.NullInt32:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Valid = false
		} else {
			//x.Valid, x.Int32 = true, int32(C.dpiData_getInt64(&data[0]))
			x.Valid, x.Int32 = true, *((*int32)(unsafe.Pointer(&data[0].value)))
		}
	case *[]sql.NullInt32:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, sql.NullInt32{Valid: false})
			} else {
				*x = append(*x, sql.NullInt32{Valid: true,
					//Int32: int32(C.dpiData_getInt64(&data[i]))})
					Int32: *((*int32)(unsafe.Pointer(&data[i].value)))})
			}
		}
	case *sql.NullInt64:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Valid = false
		} else {
			//x.Valid, x.Int64 = true, int64(C.dpiData_getInt64(&data[0]))
			x.Valid, x.Int64 = true, *((*int64)(unsafe.Pointer(&data[0].value)))
		}
	case *[]sql.NullInt64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, sql.NullInt64{Valid: false})
			} else {
				*x = append(*x, sql.NullInt64{Valid: true,
					//Int64: int64(C.dpiData_getInt64(&data[i]))})
					Int64: *((*int64)(unsafe.Pointer(&data[i].value)))})
			}
		}
	case *sql.NullFloat64:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Valid = false
		} else {
			//x.Valid, x.Float64 = true, float64(C.dpiData_getDouble(&data[0]))
			x.Valid, x.Float64 = true, *((*float64)(unsafe.Pointer(&data[0].value)))
		}
	case *[]sql.NullFloat64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, sql.NullFloat64{Valid: false})
			} else {
				*x = append(*x, sql.NullFloat64{Valid: true,
					//Float64: float64(C.dpiData_getDouble(&data[i]))})
					Float64: *((*float64)(unsafe.Pointer(&data[i].value)))})
			}
		}

	case *uint:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = uint(C.dpiData_getUint64(&data[0]))
			*x = uint(*((*uint64)(unsafe.Pointer(&data[0].value))))
		}
	case *[]uint:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, uint(C.dpiData_getUint64(&data[i])))
				*x = append(*x, uint(*((*uint64)(unsafe.Pointer(&data[i].value)))))
			}
		}
	case *[]uint8:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, uint8(C.dpiData_getUint64(&data[i])))
				*x = append(*x, *((*uint8)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *uint8:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = uint8(C.dpiData_getUint64(&data[0]))
			*x = *((*uint8)(unsafe.Pointer(&data[0].value)))
		}
	case *[]uint16:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, uint16(C.dpiData_getUint64(&data[i])))
				*x = append(*x, *((*uint16)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *uint16:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = uint16(C.dpiData_getUint64(&data[0]))
			*x = *((*uint16)(unsafe.Pointer(&data[0].value)))
		}
	case *uint32:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = uint32(C.dpiData_getUint64(&data[0]))
			*x = *((*uint32)(unsafe.Pointer(&data[0].value)))
		}
	case *[]uint32:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, uint32(C.dpiData_getUint64(&data[i])))
				*x = append(*x, *((*uint32)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *uint64:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = uint64(C.dpiData_getUint64(&data[0]))
			*x = *((*uint64)(unsafe.Pointer(&data[0].value)))
		}
	case *[]uint64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, uint64(C.dpiData_getUint64(&data[i])))
				*x = append(*x, *((*uint64)(unsafe.Pointer(&data[i].value))))
			}
		}

	case *float32:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = float32(C.dpiData_getFloat(&data[0]))
			*x = *((*float32)(unsafe.Pointer(&data[0].value)))
		}
	case *[]float32:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, float32(C.dpiData_getFloat(&data[i])))
				*x = append(*x, *((*float32)(unsafe.Pointer(&data[i].value))))
			}
		}
	case *float64:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = 0
		} else {
			//*x = float64(C.dpiData_getDouble(&data[0]))
			*x = *((*float64)(unsafe.Pointer(&data[0].value)))
		}
	case *[]float64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, 0)
			} else {
				//*x = append(*x, float64(C.dpiData_getDouble(&data[i])))
				*x = append(*x, *((*float64)(unsafe.Pointer(&data[i].value))))
			}
		}

	case *Number, *[]Number, decimalCompose, *[]decimalCompose:
		return dataGetBytes(x, data)

	default:
		return fmt.Errorf("unknown number [%T] %#v", v, v)
	}

	//fmt.Printf("setInt64(%#v, %#v)\n", data, C.int64_t(int64(v.(int))))
	return nil
}

func dataSetNumber(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	switch slice := vv.(type) {
	case int:
		i, x := 0, slice
		C.dpiData_setInt64(&data[i], C.int64_t(x))
	case []int:
		for i, x := range slice {
			C.dpiData_setInt64(&data[i], C.int64_t(x))
		}
	case int8:
		i, x := 0, slice
		C.dpiData_setInt64(&data[i], C.int64_t(x))
	case []int8:
		for i, x := range slice {
			C.dpiData_setInt64(&data[i], C.int64_t(x))
		}
	case int16:
		i, x := 0, slice
		C.dpiData_setInt64(&data[i], C.int64_t(x))
	case []int16:
		for i, x := range slice {
			C.dpiData_setInt64(&data[i], C.int64_t(x))
		}
	case int32:
		i, x := 0, slice
		C.dpiData_setInt64(&data[i], C.int64_t(x))
	case []int32:
		for i, x := range slice {
			C.dpiData_setInt64(&data[i], C.int64_t(x))
		}
	case int64:
		i, x := 0, slice
		C.dpiData_setInt64(&data[i], C.int64_t(x))
	case []int64:
		for i, x := range slice {
			C.dpiData_setInt64(&data[i], C.int64_t(x))
		}
	case sql.NullInt32:
		i, x := 0, slice
		if x.Valid {
			data[i].isNull = 0
			C.dpiData_setInt64(&data[i], C.int64_t(x.Int32))
		} else {
			data[i].isNull = 1
		}
	case []sql.NullInt32:
		for i, x := range slice {
			if x.Valid {
				data[i].isNull = 0
				C.dpiData_setInt64(&data[i], C.int64_t(x.Int32))
			} else {
				data[i].isNull = 1
			}
		}
	case sql.NullInt64:
		i, x := 0, slice
		if x.Valid {
			data[i].isNull = 0
			C.dpiData_setInt64(&data[i], C.int64_t(x.Int64))
		} else {
			data[i].isNull = 1
		}
	case []sql.NullInt64:
		for i, x := range slice {
			if x.Valid {
				data[i].isNull = 0
				C.dpiData_setInt64(&data[i], C.int64_t(x.Int64))
			} else {
				data[i].isNull = 1
			}
		}
	case sql.NullFloat64:
		i, x := 0, slice
		if x.Valid {
			data[i].isNull = 0
			C.dpiData_setDouble(&data[i], C.double(x.Float64))
		} else {
			data[i].isNull = 1
		}
	case []sql.NullFloat64:
		for i, x := range slice {
			if x.Valid {
				data[i].isNull = 0
				C.dpiData_setDouble(&data[i], C.double(x.Float64))
			} else {
				data[i].isNull = 1
			}
		}

	case uint:
		i, x := 0, slice
		C.dpiData_setUint64(&data[i], C.uint64_t(x))
	case []uint:
		for i, x := range slice {
			C.dpiData_setUint64(&data[i], C.uint64_t(x))
		}
	case uint8:
		i, x := 0, slice
		C.dpiData_setUint64(&data[i], C.uint64_t(x))
	case []uint8:
		for i, x := range slice {
			C.dpiData_setUint64(&data[i], C.uint64_t(x))
		}
	case uint16:
		i, x := 0, slice
		C.dpiData_setUint64(&data[i], C.uint64_t(x))
	case []uint16:
		for i, x := range slice {
			C.dpiData_setUint64(&data[i], C.uint64_t(x))
		}
	case uint32:
		i, x := 0, slice
		C.dpiData_setUint64(&data[i], C.uint64_t(x))
	case []uint32:
		for i, x := range slice {
			C.dpiData_setUint64(&data[i], C.uint64_t(x))
		}
	case uint64:
		i, x := 0, slice
		C.dpiData_setUint64(&data[i], C.uint64_t(x))
	case []uint64:
		for i, x := range slice {
			C.dpiData_setUint64(&data[i], C.uint64_t(x))
		}

	case float32:
		i, x := 0, slice
		C.dpiData_setFloat(&data[i], C.float(x))
	case []float32:
		for i, x := range slice {
			C.dpiData_setFloat(&data[i], C.float(x))
		}
	case float64:
		i, x := 0, slice
		C.dpiData_setDouble(&data[i], C.double(x))
	case []float64:
		for i, x := range slice {
			C.dpiData_setDouble(&data[i], C.double(x))
		}

	case Number, []Number, decimalDecompose, []decimalDecompose, string, []string:
		return dataSetBytes(dv, data, vv)

	default:
		return fmt.Errorf("unknown number slice [%T] %#v", vv, vv)
	}

	//fmt.Printf("setInt64(%#v, %#v)\n", data, C.int64_t(int64(v.(int))))
	return nil
}

func dataGetBytes(v interface{}, data []C.dpiData) error {
	switch x := v.(type) {
	case *[]byte:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = nil
			return nil
		}
		// must be copied
		*x = append((*x)[:0], dpiData_getBytes(&data[0])...)

	case *[][]byte:
		maX := (*x)[:cap(*x)]
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, nil)
				continue
			}
			b := dpiData_getBytes(&data[i])
			// b must be copied
			if i < len(maX) {
				*x = append(*x, append(maX[i][:0], b...))
			} else {
				*x = append(*x, append(make([]byte, 0, len(b)), b...))
			}
		}

	case *Number:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = ""
			return nil
		}
		*x = Number(dpiData_getBytes(&data[0]))
	case *[]Number:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, "")
				continue
			}
			*x = append(*x, Number(dpiData_getBytes(&data[i])))
		}
	case decimalCompose:
		if len(data) == 0 || data[0].isNull == 1 {
			x = nil
			return nil
		}
		return x.Compose(Number(dpiData_getBytes(&data[0])).Decompose(nil))
	case *[]decimalCompose:
		*x = (*x)[:0]
		et := reflect.TypeOf(*x).Elem()
		var a [22]byte
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, nil)
				continue
			}
			z := reflect.Zero(et).Interface().(decimalCompose)
			if err := z.Compose(Number(dpiData_getBytes(&data[i])).Decompose(a[:0])); err != nil {
				return err
			}
			*x = append(*x, z)
		}

	case *string:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = ""
			return nil
		}
		*x = string(dpiData_getBytes(&data[0]))
	case *[]string:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, "")
				continue
			}
			*x = append(*x, string(dpiData_getBytes(&data[i])))
		}

	case *sql.NullInt32:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Int32, x.Valid = 0, false
			return nil
		}
		v, err := strconv.ParseInt(string(dpiData_getBytes(&data[0])), 10, 32)
		if err != nil {
			return err
		}
		x.Int32, x.Valid = int32(v), true
		return err
	case *[]sql.NullInt32:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, sql.NullInt32{})
				continue
			}
			v, err := strconv.ParseInt(string(dpiData_getBytes(&data[i])), 10, 32)
			if err != nil {
				return err
			}
			*x = append(*x, sql.NullInt32{Valid: true, Int32: int32(v)})
		}
	case *sql.NullInt64:
		if len(data) == 0 || data[0].isNull == 1 {
			x.Int64, x.Valid = 0, false
			return nil
		}
		v, err := strconv.ParseInt(string(dpiData_getBytes(&data[0])), 10, 64)
		if err != nil {
			return err
		}
		x.Int64, x.Valid = v, true
		return nil
	case *[]sql.NullInt64:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, sql.NullInt64{})
				continue
			}
			v, err := strconv.ParseInt(string(dpiData_getBytes(&data[i])), 10, 64)
			if err != nil {
				return err
			}
			*x = append(*x, sql.NullInt64{Valid: true, Int64: v})
		}

	case *interface{}:
		switch y := (*x).(type) {
		case []byte:
			err := dataGetBytes(&y, data[:1])
			*x = y
			return err
		case [][]byte:
			err := dataGetBytes(&y, data)
			*x = y
			return err

		case Number:
			err := dataGetBytes(&y, data[:1])
			*x = y
			return err
		case []Number:
			err := dataGetBytes(&y, data)
			*x = y
			return err

		case string:
			err := dataGetBytes(&y, data[:1])
			*x = y
			return err
		case []string:
			err := dataGetBytes(&y, data)
			*x = y
			return err

		case sql.NullInt32:
			err := dataGetBytes(&y, data[:1])
			*x = y
			return err
		case []sql.NullInt32:
			err := dataGetBytes(&y, data)
			*x = y
			return err
		case sql.NullInt64:
			err := dataGetBytes(&y, data[:1])
			*x = y
			return err
		case []sql.NullInt64:
			err := dataGetBytes(&y, data)
			*x = y
			return err

		default:
			return fmt.Errorf("awaited []byte/string/Number, got %T (%#v)", x, x)
		}

	default:
		return fmt.Errorf("awaited []byte/string/Number, got %T (%#v)", v, v)
	}
	return nil
}

func dataSetBytes(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	var p *C.char
	switch slice := vv.(type) {
	case []byte:
		i, x := 0, slice
		if len(x) == 0 {
			data[i].isNull = 1
			return nil
		}
		data[i].isNull = 0
		p = (*C.char)(unsafe.Pointer(&x[0]))
		//if logger != nil {Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(x)) }
		C.dpiVar_setFromBytes(dv, C.uint32_t(i), p, C.uint32_t(len(x)))
	case [][]byte:
		for i, x := range slice {
			if len(x) == 0 {
				data[i].isNull = 1
				continue
			}
			data[i].isNull = 0
			p = (*C.char)(unsafe.Pointer(&x[0]))
			//if logger != nil {Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(x)) }
			C.dpiVar_setFromBytes(dv, C.uint32_t(i), p, C.uint32_t(len(x)))
		}

	case Number:
		i, x := 0, slice
		if len(x) == 0 {
			data[i].isNull = 1
			return nil
		}
		data[i].isNull = 0
		dpiSetFromString(dv, C.uint32_t(i), string(x))
	case []Number:
		for i, x := range slice {
			if len(x) == 0 {
				data[i].isNull = 1
				continue
			}
			data[i].isNull = 0
			dpiSetFromString(dv, C.uint32_t(i), string(x))
		}

	case decimalDecompose:
		i, x := 0, slice
		if x == nil {
			data[i].isNull = 1
			return nil
		}
		var n Number
		if err := n.Compose(x.Decompose(nil)); err != nil {
			return err
		}
		data[i].isNull = 0
		dpiSetFromString(dv, C.uint32_t(i), string(n))
	case []decimalDecompose:
		var n Number
		var a [22]byte
		for i, x := range slice {
			if x == nil {
				data[i].isNull = 1
				continue
			}
			if err := n.Compose(x.Decompose(a[:0])); err != nil {
				return err
			}
			data[i].isNull = 0
			dpiSetFromString(dv, C.uint32_t(i), string(n))
		}

	case string:
		i, x := 0, slice
		if len(x) == 0 {
			data[i].isNull = 1
			return nil
		}
		data[i].isNull = 0
		dpiSetFromString(dv, C.uint32_t(i), x)
	case []string:
		for i, x := range slice {
			if len(x) == 0 {
				data[i].isNull = 1
				continue
			}
			data[i].isNull = 0
			dpiSetFromString(dv, C.uint32_t(i), x)
		}

	default:
		return fmt.Errorf("awaited [][]byte/[]string/[]Number, got %T (%#v)", vv, vv)
	}
	return nil
}

func (st *statement) dataGetBoolBytes(v interface{}, data []C.dpiData) error {
	switch x := v.(type) {
	case *bool:
		if len(data) == 0 || data[0].isNull == 1 {
			*x = false
			return nil
		}
		*x = st.stmtOptions.boolString.FromString(string(dpiData_getBytes(&data[0])))

	case *[]bool:
		*x = (*x)[:0]
		for i := range data {
			if data[i].isNull == 1 {
				*x = append(*x, false)
				continue
			}
			*x = append(*x, st.stmtOptions.boolString.FromString(string(dpiData_getBytes(&data[i]))))
		}

	case *interface{}:
		switch y := (*x).(type) {
		case bool:
			err := st.dataGetBoolBytes(&y, data[:1])
			*x = y
			return err
		case []bool:
			err := st.dataGetBoolBytes(&y, data)
			*x = y
			return err

		default:
			return fmt.Errorf("awaited bool, got %T (%#v)", x, x)
		}

	default:
		return fmt.Errorf("awaited bool, got %T (%#v)", v, v)
	}
	return nil
}
func (st *statement) dataSetBoolBytes(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	var p *C.char
	switch slice := vv.(type) {
	case bool:
		i, x := 0, slice
		data[i].isNull = 0
		s := []byte(st.stmtOptions.boolString.ToString(x))
		p = (*C.char)(unsafe.Pointer(&s[0]))
		C.dpiVar_setFromBytes(dv, C.uint32_t(i), p, C.uint32_t(len(s)))
	case []bool:
		for i, x := range slice {
			data[i].isNull = 0
			s := []byte(st.stmtOptions.boolString.ToString(x))
			p = (*C.char)(unsafe.Pointer(&s[0]))
			C.dpiVar_setFromBytes(dv, C.uint32_t(i), p, C.uint32_t(len(s)))
		}

	default:
		return fmt.Errorf("awaited bool/[]bool, got %T (%#v)", vv, vv)
	}
	return nil
}

func (st *statement) dataGetStmt(v interface{}, data []C.dpiData) error {
	if row, ok := v.(*driver.Rows); ok {
		if len(data) == 0 || data[0].isNull == 1 {
			*row = nil
			return nil
		}
		return st.dataGetStmtC(row, &data[0])
	}
	rows := v.(*[]driver.Rows)
	if cap(*rows) >= len(data) {
		*rows = (*rows)[:len(data)]
	} else {
		*rows = make([]driver.Rows, len(data))
	}
	var firstErr error
	for i := range data {
		if err := st.dataGetStmtC(&((*rows)[i]), &data[i]); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (st *statement) dataGetStmtC(row *driver.Rows, data *C.dpiData) error {
	if data.isNull == 1 {
		*row = nil
		return nil
	}
	st2 := &statement{conn: st.conn, dpiStmt: C.dpiData_getStmt(data),
		stmtOptions: st.stmtOptions, // inherit parent statement's options
	}

	logger := getLogger()
	var n C.uint32_t
	if err := st.checkExec(func() C.int { return C.dpiStmt_getNumQueryColumns(st2.dpiStmt, &n) }); err != nil {
		err = fmt.Errorf("dataGetStmtC.getNumQueryColumns: %+v: %w", err, io.EOF)
		*row = &rows{err: err}
		if logger != nil {
			logger.Log("msg", "dataGetStmtC", "st", fmt.Sprintf("%p", st2.dpiStmt), "error", err)
		}
		return nil
	}
	r2, err := st2.openRows(int(n))
	if err != nil {
		if logger != nil {
			logger.Log("msg", "dataGetStmtC.openRows", "st", fmt.Sprintf("%p", st2.dpiStmt), "error", err)
		}
		st2.Close()
		return err
	}
	stmtSetFinalizer(st2, "dataGetStmtC")
	r2.fromData = true
	*row = r2
	return nil
}

func (c *conn) dataGetLOB(v interface{}, data []C.dpiData) error {
	if L, ok := v.(*Lob); ok {
		if len(data) == 0 || data[0].isNull == 1 {
			*L = Lob{}
			return nil
		}
		c.dataGetLOBC(L, &data[0])
		return nil
	}
	slice := v.(*[]Lob)
	n := len(data)
	if cap(*slice) >= n {
		*slice = (*slice)[:n]
	} else {
		*slice = make([]Lob, n)
	}
	for i := range data {
		c.dataGetLOBC(&((*slice)[i]), &data[i])
	}
	return nil
}
func (c *conn) dataGetLOBC(L *Lob, data *C.dpiData) {
	L.Reader = nil
	if data.isNull == 1 {
		return
	}
	lob := C.dpiData_getLOB(data)
	if lob == nil {
		return
	}
	L.Reader = &dpiLobReader{drv: c.drv, dpiLob: lob, IsClob: L.IsClob}
}

func (c *conn) dataSetLOB(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}

	lobs := []Lob{{}}
	if L, ok := vv.(Lob); ok {
		lobs[0] = L
	} else {
		lobs = vv.([]Lob)
	}
	var firstErr error
	for i, L := range lobs {
		if err := c.dataSetLOBOne(dv, data, i, L); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%d. %w", i, err)
			}
		}
	}
	return firstErr
}
func (c *conn) dataSetLOBOne(dv *C.dpiVar, data []C.dpiData, i int, L Lob) error {
	if L.Reader == nil {
		data[i].isNull = 1
		return nil
	}
	data[i].isNull = 0
	if r, ok := L.Reader.(*dpiLobReader); ok {
		if err := c.checkExec(func() C.int { return C.dpiVar_setFromLob(dv, C.uint32_t(i), r.dpiLob) }); err != nil {
			return fmt.Errorf("dpiVar_setFromLob(%p): %w", r.dpiLob, err)
		}
	}
	logger := getLogger()

	// For small reads it is faster to set it as byte slice
	var a [1 << 20]byte
	n, _ := io.ReadFull(L.Reader, a[:])
	if logger != nil {
		logger.Log("msg", "setLob", "n", n)
	}
	if n < cap(a) {
		if err := c.checkExec(func() C.int {
			return C.dpiVar_setFromBytes(dv, C.uint32_t(i), (*C.char)(unsafe.Pointer(&a[0])), C.uint32_t(n))
		}); err != nil {
			return fmt.Errorf("dpiVar_setFromBytes(%d): %w", n, err)
		}
		return nil
	}

	L.Reader = io.MultiReader(bytes.NewReader(a[:n]), L.Reader)
	typ := C.dpiOracleTypeNum(C.DPI_ORACLE_TYPE_BLOB)
	if L.IsClob {
		typ = C.DPI_ORACLE_TYPE_CLOB
	}

	var lob *C.dpiLob
	if err := c.checkExec(func() C.int { return C.dpiConn_newTempLob(c.dpiConn, typ, &lob) }); err != nil {
		return fmt.Errorf("newTempLob(typ=%d): %w", typ, err)
	}
	var chunkSize C.uint32_t
	_ = C.dpiLob_getChunkSize(lob, &chunkSize)
	if chunkSize == 0 {
		chunkSize = 8192
	}
	for chunkSize < minChunkSize {
		chunkSize <<= 1
	}
	lw := &dpiLobWriter{dpiLob: lob, drv: c.drv, isClob: L.IsClob}
	defer lw.Close() // Do NOT close before dpiVar_setFromLob !
	written, err := io.CopyBuffer(lw, L, make([]byte, int(chunkSize)))
	if logger != nil {
		logger.Log("msg", "setLOB", "written", n, "tempLob", fmt.Sprintf("%p", lob), "chunkSize", chunkSize, "error", err)
	}
	if err != nil {
		return err
	}
	{
		var lobType C.dpiOracleTypeNum
		var lobSize C.uint64_t
		err := c.checkExec(func() C.int {
			if rc := C.dpiLob_getType(lob, &lobType); rc != 0 {
				return rc
			}
			return C.dpiLob_getSize(lob, &lobSize)
		})
		if logger != nil {
			logger.Log("msg", "setLOB", "type", lobType, "size", lobSize, "error", err)
		}
		if int64(lobSize) != int64(written) {
			return fmt.Errorf("lobSize=%d, wanted %d", lobSize, written)
		}
	}
	if err = c.checkExec(func() C.int { return C.dpiVar_setFromLob(dv, C.uint32_t(i), lob) }); err != nil {
		return fmt.Errorf("dpiVar_setFromLob(%d. %p): %w", i, lob, err)
	}
	return nil
}

type userType interface {
	ObjectRef() *Object
}

// ObjectScanner assigns a value from a database object
type ObjectScanner interface {
	sql.Scanner
	userType
}

// ObjectWriter update database object before binding
type ObjectWriter interface {
	WriteObject() error
	userType
}

func (c *conn) dataSetObject(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	//fmt.Printf("\ndataSetObject(dv=%+v, data=%+v, vv=%+v)\n", dv, data, vv)
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	objs := []Object{{}}
	switch o := vv.(type) {
	case Object:
		objs[0] = o
	case *Object:
		objs[0] = *o
	case []Object:
		objs = o
	case []*Object:
		objs = make([]Object, len(o))
		for i, x := range o {
			objs[i] = *x
		}
	case ObjectWriter:
		err := o.WriteObject()
		if err != nil {
			return err
		}
		objs[0] = *o.ObjectRef()
	case []ObjectWriter:
		for _, ut := range o {
			err := ut.WriteObject()
			if err != nil {
				return err
			}
			objs = append(objs, *ut.ObjectRef())
		}
	case userType:
		objs[0] = *o.ObjectRef()
	case []userType:
		for _, ut := range o {
			objs = append(objs, *ut.ObjectRef())
		}
	}
	for i, obj := range objs {
		if obj.dpiObject == nil {
			data[i].isNull = 1
			continue
		}
		data[i].isNull = 0
		if err := c.checkExec(func() C.int { return C.dpiVar_setFromObject(dv, C.uint32_t(i), obj.dpiObject) }); err != nil {
			return fmt.Errorf("setFromObject: %w", err)
		}
	}
	return nil
}

func (c *conn) dataGetObject(v interface{}, data []C.dpiData) error {
	logger := getLogger()
	switch out := v.(type) {
	case *Object:
		d := Data{
			ObjectType: out.ObjectType,
			dpiData:    data[0],
		}
		if logger != nil {
			logger.Log("msg", "dataGetObject", "v", fmt.Sprintf("%T", v), "d", d)
		}
		obj := d.GetObject()
		if obj == nil {
			*out = Object{ObjectType: d.ObjectType}
		} else {
			*out = *obj
		}
	case *ObjectCollection:
		d := Data{
			ObjectType: out.Collection().ObjectType,
			dpiData:    data[0],
		}
		if logger != nil {
			logger.Log("msg", "dataGetObject", "v", fmt.Sprintf("%T", v), "d", d)
		}
		obj := d.GetObject()
		if obj == nil {
			*out = ObjectCollection{}
		} else {
			// copy the underlying object
			obj2 := *obj
			*out = ObjectCollection{Object: &obj2}
		}
	case ObjectScanner:
		d := Data{
			ObjectType: out.ObjectRef().ObjectType,
			dpiData:    data[0],
		}
		if logger != nil {
			logger.Log("msg", "dataGetObjectScanner", "v", fmt.Sprintf("%T", v), "d", d, "obj", d.GetObject())
		}
		obj := d.GetObject()
		err := out.Scan(obj)
		obj.Close()
		return err
	default:

		return fmt.Errorf("dataGetObject not implemented for type %T (maybe you need to implement the Scan method)", v)
	}

	return nil
}

func (c *conn) dataGetJSON(v interface{}, data []C.dpiData) error {

	switch out := v.(type) {
	case *JSON:
		*out = JSON{dpiJson: *((**C.dpiJson)(unsafe.Pointer(&(data[0].value))))}
	default:
		return fmt.Errorf("dataGetJSONNode not implemented for type %T", v)
	}
	return nil
}

func (c *conn) dataSetJSON(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	i := 0
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	switch x := vv.(type) {
	case JSON:
		*((**C.dpiJson)(unsafe.Pointer(&(data[0].value)))) = x.dpiJson

		C.dpiVar_setFromJson(dv, C.uint32_t(i), *((**C.dpiJson)(unsafe.Pointer(&(data[0].value)))))
	case []JSON:
		for i := range x {
			*((*C.dpiJson)(unsafe.Pointer(&(data[i].value)))) = *x[i].dpiJson
		}
	default:
		return fmt.Errorf("dataSetJSONArray not implemented for type %T", x)
	}
	return nil
}

func (c *conn) dataSetJSONValue(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	var err error = nil
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	switch x := vv.(type) {
	case JSONValue:
		v := reflect.ValueOf(x.Value)
		t := v.Type()
		switch t.Kind() {
		case reflect.Map, reflect.String, reflect.Slice, reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
			reflect.Uint32, reflect.Uint64, reflect.Float32,
			reflect.Float64:
			data[0].isNull = 0
			var dpijsonnode *C.dpiJsonNode
			err = allocdpiJSONNode(x.Value, &dpijsonnode)
			if err != nil {
				return fmt.Errorf("dataSetJSONValue %w", err)
			}
			defer freedpiJSONNode(dpijsonnode)
			if err = c.checkExec(func() C.int { return C.dpiJson_setValue(C.dpiData_getJson(&(data[0])), dpijsonnode) }); err != nil {
				return fmt.Errorf("dataSetJSONValue %w", err)
			}
		default:
			return fmt.Errorf("dataSetJSONValue Unsupported JSON doc type %#v: ", t.Name())
		}
	case []JSONValue:
		for i := range x {
			data[i].isNull = 0

			v := reflect.ValueOf(x[i].Value)
			t := v.Type()
			switch t.Kind() {
			case reflect.Map, reflect.String, reflect.Slice, reflect.Bool,
				reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
				reflect.Uint32, reflect.Uint64, reflect.Float32,
				reflect.Float64:
				var dpijsonnode *C.dpiJsonNode
				err = allocdpiJSONNode(x[i].Value, &dpijsonnode)
				if err != nil {
					return fmt.Errorf("dataSetJSONValue[%d] %w", i, err)
				}
				defer freedpiJSONNode(dpijsonnode)
				if err = c.checkExec(func() C.int { return C.dpiJson_setValue(C.dpiData_getJson(&(data[i])), dpijsonnode) }); err != nil {
					return fmt.Errorf("dataSetJSONValue[%d] %w", i, err)
				}
			default:
				return fmt.Errorf("dataSetJSONValue Unsupported JSON doc[%d] type %#v: ", i, t.Name())
			}
		}
	default:
		return fmt.Errorf("dataSetJSONValue not implemented for type %T", x)
	}
	return err
}

func (c *conn) dataGetJSONValue(v interface{}, data []C.dpiData) error {
	switch out := v.(type) {
	case *JSON:
		*out = JSON{dpiJson: (*(**C.dpiJson)(unsafe.Pointer(&(data[0].value))))}
	default:
		return fmt.Errorf("dataGetJSONValue not implemented for type %T", out)
	}
	return nil
}

func (c *conn) dataSetJSONString(dv *C.dpiVar, data []C.dpiData, vv interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if vv == nil {
		return dataSetNull(dv, data, nil)
	}
	switch js := vv.(type) {
	case JSONString:
		if len(js.Value) == 0 {
			data[0].isNull = 1
			return nil
		}
		cstr := C.CString(js.Value)
		defer C.free(unsafe.Pointer(cstr))
		data[0].isNull = 0
		if err := c.checkExec(func() C.int {
			return C.dpiJson_setFromText(C.dpiData_getJson(&(data[0])), cstr, C.uint64_t(len(js.Value)), C.uint32_t(js.Flags))
		}); err != nil {
			return fmt.Errorf("setFromJsonString(string=%#v): %w", vv, err)
		}
	case []JSONString:
		for i := range js {
			if len(js[i].Value) == 0 {
				data[0].isNull = 1
				continue
			}
			data[i].isNull = 0
			cstr := C.CString(js[i].Value)
			defer C.free(unsafe.Pointer(cstr))
			if err := c.checkExec(func() C.int {
				return C.dpiJson_setFromText(C.dpiData_getJson(&(data[i])), cstr, C.uint64_t(len(js[i].Value)), C.uint32_t(js[i].Flags))
			}); err != nil {
				return fmt.Errorf("setFromJsonString(string=%#v): %w", js[i].Value, err)
			}
		}
	default:
		return fmt.Errorf("setFromJsonString Unsupported JSON string [%T] %#v", vv, vv)
	}
	return nil
}

func (c *conn) dataGetJSONString(v interface{}, data []C.dpiData) error {

	switch out := v.(type) {
	case *string:
		js := JSON{dpiJson: (*(**C.dpiJson)(unsafe.Pointer(&(data[0].value))))}
		*out = js.String()
	default:
		return fmt.Errorf("dataGetJSONString not implemented for type %T", out)
	}
	return nil
}

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrBadDate        = errors.New("date out of range (year must be between -4713 and 9999, and must not be 0)")
)

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
//
// If CheckNamedValue returns ErrRemoveArgument, the NamedValue will not be included
// in the final query arguments.
// This may be used to pass special options to the query itself.
//
// If ErrSkip is returned the column converter error checking path is used
// for the argument.
// Drivers may wish to return ErrSkip after they have exhausted their own special cases.
func (st *statement) CheckNamedValue(nv *driver.NamedValue) error {
	if nv == nil {
		return nil
	}
	if apply, ok := nv.Value.(Option); ok {
		if apply != nil {
			apply(&st.stmtOptions)
		}
		return driver.ErrRemoveArgument
	}
	return nil
}

// ColumnConverter may be optionally implemented by Stmt
// if the statement is aware of its own columns' types and
// can convert from any type to a driver Value.
func (st *statement) ColumnConverter(idx int) driver.ValueConverter {
	c := driver.ValueConverter(driver.DefaultParameterConverter)
	switch col := st.columns[idx]; col.OracleType {
	case C.DPI_ORACLE_TYPE_NUMBER:
		switch col.NativeType {
		case C.DPI_NATIVE_TYPE_INT64, C.DPI_NATIVE_TYPE_UINT64:
			c = Int64
		//case C.DPI_NATIVE_TYPE_FLOAT, C.DPI_NATIVE_TYPE_DOUBLE:
		//	c = Float64
		default:
			c = Num
		}
	}
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "ColumnConverter", "c", c)
	}
	return driver.Null{Converter: c}
}

func (st *statement) openRows(colCount int) (*rows, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	sliceLen := st.FetchArraySize()

	r := rows{
		statement: st,
		columns:   make([]Column, colCount),
		vars:      make([]*C.dpiVar, colCount),
		data:      make([][]C.dpiData, colCount),
	}

	var info C.dpiQueryInfo
	var ti C.dpiDataTypeInfo
	logger := getLogger()
	for i := 0; i < colCount; i++ {
		if err := st.checkExecNoLOT(func() C.int {
			return C.dpiStmt_getQueryInfo(st.dpiStmt, C.uint32_t(i+1), &info)
		}); err != nil {
			return nil, fmt.Errorf("getQueryInfo[%d]: %w", i, err)
		}
		ti = info.typeInfo
		bufSize := int(ti.clientSizeInBytes)
		if logger != nil {
			logger.Log("msg", "openRows", "col", i, "info", ti)
		}
		//if logger != nil {Log("dNTN", int(ti.defaultNativeTypeNum), "number", C.DPI_ORACLE_TYPE_NUMBER) }
		effTypeNum := ti.oracleTypeNum
		switch effTypeNum {
		case C.DPI_ORACLE_TYPE_NUMBER:
			switch ti.defaultNativeTypeNum {
			case C.DPI_NATIVE_TYPE_FLOAT, C.DPI_NATIVE_TYPE_DOUBLE:
				ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
				bufSize = 40
			}
		case C.DPI_ORACLE_TYPE_DATE,
			C.DPI_ORACLE_TYPE_TIMESTAMP, C.DPI_ORACLE_TYPE_TIMESTAMP_TZ, C.DPI_ORACLE_TYPE_TIMESTAMP_LTZ:
			ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_TIMESTAMP

		case C.DPI_ORACLE_TYPE_BLOB:
			if !st.LobAsReader() {
				effTypeNum = C.DPI_ORACLE_TYPE_LONG_RAW
				ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
			}
		case C.DPI_ORACLE_TYPE_CLOB:
			if !st.LobAsReader() {
				effTypeNum = C.DPI_ORACLE_TYPE_LONG_VARCHAR
				ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
			}
		}
		r.columns[i] = Column{
			Name:           C.GoStringN(info.name, C.int(info.nameLength)),
			OracleType:     effTypeNum,
			OrigOracleType: ti.oracleTypeNum,
			NativeType:     ti.defaultNativeTypeNum,
			Size:           ti.clientSizeInBytes,
			Precision:      ti.precision,
			Scale:          ti.scale,
			Nullable:       info.nullOk == 1,
			ObjectType:     ti.objectType,
			SizeInChars:    ti.sizeInChars,
			DBSize:         ti.dbSizeInBytes,
		}
		var err error
		//fmt.Printf("%d. %+v\n", i, r.columns[i])
		vi := varInfo{
			Typ:        effTypeNum,
			NatTyp:     ti.defaultNativeTypeNum,
			ObjectType: ti.objectType,
			BufSize:    bufSize,
			SliceLen:   sliceLen,
		}
		if r.vars[i], r.data[i], err = st.newVar(vi); err != nil {
			return nil, err
		}

		if err = st.checkExecNoLOT(func() C.int {
			return C.dpiStmt_define(st.dpiStmt, C.uint32_t(i+1), r.vars[i])
		}); err != nil {
			return nil, fmt.Errorf("define[%d]: %w", i, err)
		}
	}
	if err := st.checkExecNoLOT(func() C.int {
		return C.dpiStmt_addRef(st.dpiStmt)
	}); err != nil {
		return &r, fmt.Errorf("dpiStmt_addRef: %w", err)
	}
	st.columns = r.columns
	return &r, nil
}

// Column holds the info from a column.
type Column struct {
	ObjectType                 *C.dpiObjectType
	Name                       string
	OracleType, OrigOracleType C.dpiOracleTypeNum
	NativeType                 C.dpiNativeTypeNum
	Size, SizeInChars, DBSize  C.uint32_t
	Precision                  C.int16_t
	Scale                      C.int8_t
	Nullable                   bool
}

func dpiSetFromString(dv *C.dpiVar, pos C.uint32_t, x string) {
	C.godror_setFromString(dv, pos, x)
}

var stringBuilders = stringBuilderPool{
	p: &sync.Pool{New: func() interface{} { return &strings.Builder{} }},
}

type stringBuilderPool struct {
	p *sync.Pool
}

func (sb stringBuilderPool) Get() *strings.Builder {
	return sb.p.Get().(*strings.Builder)
}
func (sb *stringBuilderPool) Put(b *strings.Builder) {
	b.Reset()
	sb.p.Put(b)
}

func isInvalidErr(err error) bool {
	var cdr interface{ Code() int }
	if err == nil || !errors.As(err, &cdr) {
		return false
	}
	code := cdr.Code()
	// ORA-04068: "existing state of packages has been discarded"
	return code == 4061 || code == 4065 || code == 4068
}

/*
// ResetSession is called while a connection is in the connection
// pool. No queries will run on this connection until this method returns.
//
// If the connection is bad this should return driver.ErrBadConn to prevent
// the connection from being returned to the connection pool. Any other
// error will be discarded.
func (c *conn) ResetSession(ctx context.Context) error {
	if logger := ctxGetLog(ctx); logger != nil {
		logger.Log("msg", "ResetSession", "conn", c.dpiConn)
	}
	//subCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	//err := c.Ping(subCtx)
	//cancel()
	return c.Ping(ctx)
}
*/

var maxStackSize uint32 = 2048

func stmtSetFinalizer(st *statement, tag string) {
	// Store the current stack for printing later.
	n := atomic.LoadUint32(&maxStackSize)
	var stack []byte
	for {
		stack = make([]byte, n)
		stack = stack[:runtime.Stack(stack, false)]
		if len(stack) < cap(stack) {
			break
		}
		n *= 2
	}
	if atomic.LoadUint32(&maxStackSize) < n {
		atomic.StoreUint32(&maxStackSize, n)
	}

	runtime.SetFinalizer(st, func(st *statement) {
		if st != nil && st.dpiStmt != nil {
			if logger := getLogger(); logger != nil {
				logger.Log("msg", "ERROR: statement is not closed!", "stmt", st, "tag", tag, "stack", string(stack))
			} else {
				fmt.Printf("ERROR: statement %p of %s is not closed!\n%s\n", st, tag, stack)
			}
			st.closeNotLocking()
		}
	})
}

func dpiDataSlice(data *C.dpiData, n C.uint) []C.dpiData {
	// https://github.com/golang/go/wiki/cgo#Turning_C_arrays_into_Go_slices
	/*
		var theCArray *C.YourType = C.getTheArray()
		length := C.getTheArrayLength()
		slice := (*[maxArraySize]C.YourType)(unsafe.Pointer(theCArray))[:length:length]
	*/
	return (*(*[maxArraySize]C.dpiData)(unsafe.Pointer(data)))[:int(n):int(n)]
}

func dpiData_getBytes(data *C.dpiData) []byte {
	db := ((*C.dpiBytes)(unsafe.Pointer(&data.value)))
	return ((*[32767]byte)(unsafe.Pointer(db.ptr)))[:db.length:db.length]
}
