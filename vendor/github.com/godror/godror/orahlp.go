// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"time"
)

// Number as string
type Number string

var (
	// Int64 for converting to-from int64.
	Int64 = intType{}
	// Float64 for converting to-from float64.
	Float64 = floatType{}
	// Num for converting to-from Number (string)
	Num = numType{}
)

type intType struct{}

func (intType) String() string { return "Int64" }
func (intType) ConvertValue(v interface{}) (driver.Value, error) {
	logger := getLogger()
	if logger != nil {
		logger.Log("ConvertValue", "Int64", "value", v)
	}
	switch x := v.(type) {
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float32:
		if _, f := math.Modf(float64(x)); f != 0 {
			return int64(x), fmt.Errorf("non-zero fractional part: %f", f)
		}
		return int64(x), nil
	case float64:
		if _, f := math.Modf(x); f != 0 {
			return int64(x), fmt.Errorf("non-zero fractional part: %f", f)
		}
		return int64(x), nil
	case string:
		if x == "" {
			return 0, nil
		}
		return strconv.ParseInt(x, 10, 64)
	case Number:
		if x == "" {
			return 0, nil
		}
		return strconv.ParseInt(string(x), 10, 64)
	case *Number:
		if x == nil || *x == "" {
			return 0, nil
		}
		return strconv.ParseInt(string(*x), 10, 64)
	default:
		return nil, fmt.Errorf("unknown type %T", v)
	}
}

type floatType struct{}

func (floatType) String() string { return "Float64" }
func (floatType) ConvertValue(v interface{}) (driver.Value, error) {
	logger := getLogger()
	if logger != nil {
		logger.Log("ConvertValue", "Float64", "value", v)
	}
	switch x := v.(type) {
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		if x == "" {
			return 0, nil
		}
		return strconv.ParseFloat(x, 64)
	case Number:
		if x == "" {
			return 0, nil
		}
		return strconv.ParseFloat(string(x), 64)
	case *Number:
		if x == nil || *x == "" {
			return 0, nil
		}
		return strconv.ParseFloat(string(*x), 64)
	default:
		return nil, fmt.Errorf("unknown type %T", v)
	}
}

type numType struct{}

func (numType) String() string { return "Num" }
func (numType) ConvertValue(v interface{}) (driver.Value, error) {
	logger := getLogger()
	if logger != nil {
		logger.Log("ConvertValue", "Num", "value", v)
	}
	switch x := v.(type) {
	case string:
		if x == "" {
			return 0, nil
		}
		return x, nil
	case Number:
		if x == "" {
			return 0, nil
		}
		return string(x), nil
	case *Number:
		if x == nil || *x == "" {
			return 0, nil
		}
		return string(*x), nil
	case int8, int16, int32, int64, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x), nil
	case float32, float64:
		return fmt.Sprintf("%f", x), nil
	case decimalDecompose:
		var n Number
		err := n.Compose(x.Decompose(nil))
		return string(n), err
	default:
		return nil, fmt.Errorf("unknown type %T", v)
	}
}
func (n Number) String() string { return string(n) }

// Value returns the Number as driver.Value
func (n Number) Value() (driver.Value, error) {
	return string(n), nil
}

// Scan into the Number from a driver.Value.
func (n *Number) Scan(v interface{}) error {
	if v == nil {
		*n = ""
		return nil
	}
	switch x := v.(type) {
	case string:
		*n = Number(x)
	case Number:
		*n = x
	case *Number:
		if x == nil {
			*n = ""
		} else {
			*n = *x
		}
	case int8, int16, int32, int64, uint16, uint32, uint64:
		*n = Number(fmt.Sprintf("%d", x))
	case float32, float64:
		*n = Number(fmt.Sprintf("%f", x))
	case decimalDecompose:
		return n.Compose(x.Decompose(nil))
	default:
		return fmt.Errorf("unknown type %T", v)
	}
	return nil
}

// MarshalText marshals a Number to text.
func (n Number) MarshalText() ([]byte, error) {
	if len(n) > 40 {
		return nil, nil
	}
	return []byte(n), nil
}

// UnmarshalText parses text into a Number.
func (n *Number) UnmarshalText(p []byte) error {
	*n = ""
	if len(p) == 0 || len(p) > 40 {
		return nil
	}
	var dotNum int
	for i, c := range p {
		if !(c == '-' && i == 0 || '0' <= c && c <= '9') {
			if c == '.' {
				dotNum++
				if dotNum == 1 {
					continue
				}
			}
			return fmt.Errorf("unknown char %c in %q", c, p)
		}
	}
	*n = Number(p)
	return nil
}

// MarshalJSON marshals a Number into a JSON string.
func (n Number) MarshalJSON() ([]byte, error) {
	if len(n) > 40 {
		return []byte("null"), nil
	}
	return append(append(append(make([]byte, 0, 1+len(n)+1), '"'), []byte(n)...), '"'), nil
}

// UnmarshalJSON parses a JSON string into the Number.
func (n *Number) UnmarshalJSON(p []byte) error {
	*n = Number("")
	if len(p) == 0 || len(p) > 40 {
		return nil
	}
	if len(p) > 2 && p[0] == '"' && p[len(p)-1] == '"' {
		p = p[1 : len(p)-1]
	}
	return n.UnmarshalText(p)
}

// QueryColumn is the described column.
type QueryColumn struct {
	Name                           string
	Type, Length, Precision, Scale int
	Nullable                       bool
	//Schema string
	//CharsetID, CharsetForm         int
}

// Execer is the ExecContext of sql.Conn.
type Execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// Querier is the QueryContext of sql.Conn.
type Querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// DescribeQuery describes the columns in the qry.
//
// This can help using unknown-at-compile-time, a.k.a.
// dynamic queries.
func DescribeQuery(ctx context.Context, db Execer, qry string) ([]QueryColumn, error) {
	var cols []QueryColumn
	err := Raw(ctx, db, func(c Conn) error {
		stmt, err := c.PrepareContext(ctx, qry)
		if err != nil {
			return err
		}
		defer stmt.Close()
		st := stmt.(*statement)
		describeOnly(&st.stmtOptions)
		dR, err := st.QueryContext(ctx, nil)
		if err != nil {
			return err
		}
		defer dR.Close()
		r := dR.(*rows)
		cols = make([]QueryColumn, len(r.columns))
		for i, col := range r.columns {
			cols[i] = QueryColumn{
				Name:      col.Name,
				Type:      int(col.OracleType),
				Length:    int(col.Size),
				Precision: int(col.Precision),
				Scale:     int(col.Scale),
				Nullable:  col.Nullable,
			}
		}
		return nil
	})
	return cols, err
}

// CompileError represents a compile-time error as in user_errors view.
type CompileError struct {
	Owner, Name, Type    string
	Line, Position, Code int64
	Text                 string
	Warning              bool
}

func (ce CompileError) Error() string {
	prefix := "ERROR "
	if ce.Warning {
		prefix = "WARN  "
	}
	return fmt.Sprintf("%s %s.%s %s %d:%d [%d] %s",
		prefix, ce.Owner, ce.Name, ce.Type, ce.Line, ce.Position, ce.Code, ce.Text)
}

// GetCompileErrors returns the slice of the errors in user_errors.
//
// If all is false, only errors are returned; otherwise, warnings, too.
func GetCompileErrors(ctx context.Context, queryer Querier, all bool) ([]CompileError, error) {
	rows, err := queryer.QueryContext(ctx, `
	SELECT USER owner, name, type, line, position, message_number, text, attribute
		FROM user_errors
		ORDER BY name, sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var errors []CompileError
	var warn string
	for rows.Next() {
		var ce CompileError
		if err = rows.Scan(&ce.Owner, &ce.Name, &ce.Type, &ce.Line, &ce.Position, &ce.Code, &ce.Text, &warn); err != nil {
			return errors, err
		}
		ce.Warning = warn == "WARNING"
		if !ce.Warning || all {
			errors = append(errors, ce)
		}
	}
	return errors, rows.Err()
}

type preparer interface {
	PrepareContext(ctx context.Context, qry string) (*sql.Stmt, error)
}

// NamedToOrdered converts the query from named params (:paramname) to :%d placeholders + slice of params, copying the params verbatim.
func NamedToOrdered(qry string, namedParams map[string]interface{}) (string, []interface{}) {
	return MapToSlice(qry, func(k string) interface{} { return namedParams[k] })
}

// MapToSlice modifies query for map (:paramname) to :%d placeholders + slice of params.
//
// Calls metParam for each parameter met, and returns the slice of their results.
func MapToSlice(qry string, metParam func(string) interface{}) (string, []interface{}) {
	if metParam == nil {
		metParam = func(string) interface{} { return nil }
	}
	arr := make([]interface{}, 0, 16)
	var buf bytes.Buffer
	state, p, last := 0, 0, 0
	var prev rune

	Add := func(i int) {
		state = 0
		if i-p <= 1 { // :=
			return
		}
		arr = append(arr, metParam(qry[p+1:i]))
		param := fmt.Sprintf(":%d", len(arr))
		buf.WriteString(qry[last:p])
		buf.WriteString(param)
		last = i
	}

	for i, r := range qry {
		switch state {
		case 2:
			if r == '\n' {
				state = 0
			}
		case 3:
			if prev == '*' && r == '/' {
				state = 0
			}
		case 4:
			if r == '\'' {
				state = 0
			}
		case 0:
			switch r {
			case '-':
				if prev == '-' {
					state = 2
				}
			case '*':
				if prev == '/' {
					state = 3
				}
			case '\'':
				state = 4
			case ':':
				state = 1
				p = i
				// An identifier consists of a letter optionally followed by more letters, numerals, dollar signs, underscores, and number signs.
				// http://docs.oracle.com/cd/B19306_01/appdev.102/b14261/fundamentals.htm#sthref309
			}
		case 1:
			if !('A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' ||
				(i-p > 1 && ('0' <= r && r <= '9' || r == '$' || r == '_' || r == '#'))) {

				Add(i)
			}
		}
		prev = r
	}
	if state == 1 {
		Add(len(qry))
	}
	if last <= len(qry)-1 {
		buf.WriteString(qry[last:])
	}
	return buf.String(), arr
}

// EnableDbmsOutput enables DBMS_OUTPUT buffering on the given connection.
// This is required if you want to retrieve the output with ReadDbmsOutput later.
//
// Warning! EnableDbmsOutput, the code that uses DBMS_OUTPUT and ReadDbmsOutput
// must all execute on the same session - for example by using the same *sql.Tx,
// or *sql.Conn. A *sql.DB connection pool won't work!
func EnableDbmsOutput(ctx context.Context, conn Execer) error {
	qry := "BEGIN DBMS_OUTPUT.enable(NULL); END;"
	_, err := conn.ExecContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	return nil
}

// ReadDbmsOutput copies the DBMS_OUTPUT buffer into the given io.Writer.
//
// Be sure that you enable it beforehand (either with EnableDbmsOutput or with DBMS_OUTPUT.enable(NULL))
//
// Warning! EnableDbmsOutput, the code that uses DBMS_OUTPUT and ReadDbmsOutput
// must all execute on the same session - for example by using the same *sql.Tx,
// or *sql.Conn. A *sql.DB connection pool won't work!
func ReadDbmsOutput(ctx context.Context, w io.Writer, conn preparer) error {
	const maxNumLines = 128
	bw := bufio.NewWriterSize(w, maxNumLines*(32<<10))

	const qry = `BEGIN DBMS_OUTPUT.get_lines(:1, :2); END;`
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer stmt.Close()

	lines := make([]string, maxNumLines)
	var numLines int64
	params := []interface{}{
		PlSQLArrays,
		sql.Out{Dest: &lines}, sql.Out{Dest: &numLines, In: true},
	}
	for {
		numLines = int64(len(lines))
		if _, err = stmt.ExecContext(ctx, params...); err != nil {
			_ = bw.Flush()
			return fmt.Errorf("%s: %w", qry, err)
		}
		if numLines == 0 {
			break
		}
		for i := 0; i < int(numLines); i++ {
			_, _ = bw.WriteString(lines[i])
			if err = bw.WriteByte('\n'); err != nil {
				_ = bw.Flush()
				return err
			}
		}
		if int(numLines) < len(lines) {
			break
		}
	}
	return bw.Flush()
}

// ClientVersion returns the VersionInfo from the DB.
func ClientVersion(ctx context.Context, ex Execer) (vi VersionInfo, err error) {
	err = Raw(ctx, ex, func(c Conn) error {
		vi, err = c.ClientVersion()
		return err
	})
	return vi, err
}

// ServerVersion returns the VersionInfo of the client.
func ServerVersion(ctx context.Context, ex Execer) (vi VersionInfo, err error) {
	err = Raw(ctx, ex, func(c Conn) error {
		vi, err = c.ServerVersion()
		return err
	})
	return vi, err
}

// Conn is the interface for a connection, to be returned by DriverConn.
type Conn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.Pinger

	Break() error
	Commit() error
	Rollback() error

	ClientVersion() (VersionInfo, error)
	ServerVersion() (VersionInfo, error)
	Startup(StartupMode) error
	Shutdown(ShutdownMode) error

	NewSubscription(string, func(Event), ...SubscriptionOption) (*Subscription, error)
	GetObjectType(name string) (*ObjectType, error)
	NewData(baseType interface{}, SliceLen, BufSize int) ([]*Data, error)
	NewTempLob(isClob bool) (*DirectLob, error)

	Timezone() *time.Location
	GetPoolStats() (PoolStats, error)
}

// WrapRows transforms a driver.Rows into an *sql.Rows.
func WrapRows(ctx context.Context, q Querier, rset driver.Rows) (*sql.Rows, error) {
	return q.QueryContext(ctx, wrapResultset, rset)
}

// Timezone returns the timezone of the connection (database).
func Timezone(ctx context.Context, ex Execer) (loc *time.Location, err error) {
	err = Raw(ctx, ex, func(c Conn) error { loc = c.Timezone(); return nil })
	return loc, err
}

// DriverConn will return the connection of ex.
// For connection pools (*sql.DB) this may be a new connection.
func DriverConn(ctx context.Context, ex Execer) (Conn, error) {
	return getConn(ctx, ex)
}

var getConnMu sync.Mutex

// getConn will acquire a separate connection to the same DB as what ex is connected to.
func getConn(ctx context.Context, ex Execer) (*conn, error) {
	getConnMu.Lock()
	defer getConnMu.Unlock()
	var c interface{}
	if _, err := ex.ExecContext(ctx, getConnection, sql.Out{Dest: &c}); err != nil {
		return nil, fmt.Errorf("getConnection: %w", err)
	} else if c == nil {
		return nil, errors.New("nil connection")
	}
	return c.(*conn), nil
}

// Raw executes f on the given *sql.DB or *sql.Conn.
func Raw(ctx context.Context, ex Execer, f func(driverConn Conn) error) error {
	sf := func(driverConn interface{}) error { return f(driverConn.(Conn)) }
	if rawer, ok := ex.(interface {
		Raw(func(interface{}) error) error
	}); ok {
		return rawer.Raw(sf)
	}
	var err error
	if conner, ok := ex.(interface {
		Conn(context.Context) (*sql.Conn, error)
	}); ok {
		conn, cErr := conner.Conn(ctx)
		if cErr != nil {
			return cErr
		}
		defer conn.Close()
		return conn.Raw(sf)
	}
	if txer, ok := ex.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}); ok {
		tx, txErr := txer.BeginTx(ctx, nil)
		if txErr != nil {
			return txErr
		}
		defer func() {
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}()
		ex = tx
	}

	var cx *conn
	if cx, err = getConn(ctx, ex); err != nil {
		return err
	}
	defer cx.Close()
	return f(cx)
}
