// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

// Package godror is a database/sql/driver for Oracle DB.
//
// The connection string for the sql.Open("godror", dataSourceName) call can be
// the simple
//
//   user="login" password="password" connectString="host:port/service_name" sysdba=true
//
// with additional params (here with the defaults):
//     sysdba=0
//     sysoper=0
//     poolMinSessions=1
//     poolMaxSessions=1000
//     poolMaxSessionsPerShard=
//     poolPingInterval=
//     poolIncrement=1
//     connectionClass=
//     standaloneConnection=0
//     enableEvents=0
//     heterogeneousPool=0
//     externalAuth=0
//     prelim=0
//     poolWaitTimeout=5m
//     poolSessionMaxLifetime=1h
//     poolSessionTimeout=30s
//     timezone=
//     noTimezoneCheck=
//     newPassword=
//     onInit="ALTER SESSION SET current_schema=my_schema"
//     configDir=
//     libDir=
//     stmtCacheSize=
//     charset=UTF-8
//
// These are the defaults.
// For external authentication, user and password should be empty
// with default value(0) for heterogeneousPool parameter.
// heterogeneousPool(valid for standaloneConnection=0)
// and externalAuth parameters are internally set. For Proxy
// support , sessionuser is enclosed in brackets [sessionuser].
//
// To use a heterogeneous Pool with Proxy Support ,user and password
// parameters should be non-empty and parameter heterogeneousPool should be 1.
// If user,password are empty and heterogeneousPool is set to 1,
// different user and password can be passed in subsequent queries.
//
// Many advocate that a static session pool (min=max, incr=0)
// is better, with 1-10 sessions per CPU thread.
// See https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-7DFBA826-7CC0-4D16-B19C-31D168069B54
// You may also use ConnectionParams to configure a connection.
//
// If you specify connectionClass, that'll reuse the same session pool
// without the connectionClass, but will specify it on each session acquire.
// Thus you can cluster the session pool with classes.
//
// For connectionClass usage, see https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-CE6E4DCC-92DF-4946-92B8-2BDD9845DA35
//
// If you specify server_type as POOLED in sid, DRCP is used.
// For what can be used as "sid", see https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-E5358DEA-D619-4B7B-A799-3D2F802500F1
//
// Go strings are UTF-8, so the default charset should be used unless there's a really good reason to interfere with Oracle's character set conversion.
package godror

/*
#cgo CFLAGS: -I./odpi/include -I./odpi/src -I./odpi/embed

#include "dpi.c"

*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/godror/godror/dsn"
)

const (
	// DefaultFetchArraySize is the fetch array size by default (if not changed through FetchArraySize statement option).
	DefaultFetchArraySize = C.DPI_DEFAULT_FETCH_ARRAY_SIZE

	// DefaultPrefetchCountis the number of prefetched rows by default (if not changed through PrefetchCount statement option).
	DefaultPrefetchCount = DefaultFetchArraySize

	// DefaultArraySize is the length of the maximum PL/SQL array by default (if not changed through ArraySize statement option).
	DefaultArraySize = 1 << 10
)

// DriverName is set on the connection to be seen in the DB
//
// It cannot be longer than 30 bytes !
var DriverName = "godror : " + Version

const (
	// DpiMajorVersion is the wanted major version of the underlying ODPI-C library.
	DpiMajorVersion = C.DPI_MAJOR_VERSION
	// DpiMinorVersion is the wanted minor version of the underlying ODPI-C library.
	DpiMinorVersion = C.DPI_MINOR_VERSION
	// DpiPatchLevel is the patch level version of the underlying ODPI-C library
	DpiPatchLevel = C.DPI_PATCH_LEVEL
	// DpiVersionNumber is the underlying ODPI-C version as one number (Major * 10000 + Minor * 100 + Patch)
	DpiVersionNumber = C.DPI_VERSION_NUMBER

	// DefaultPoolMinSessions specifies the default value for minSessions for pool creation.
	DefaultPoolMinSessions = dsn.DefaultPoolMinSessions
	// DefaultPoolMaxSessions specifies the default value for maxSessions for pool creation.
	DefaultPoolMaxSessions = dsn.DefaultPoolMaxSessions
	// DefaultSessionIncrement specifies the default value for increment for pool creation.
	DefaultSessionIncrement = dsn.DefaultSessionIncrement
	// DefaultPoolIncrement is a deprecated name for DefaultSessionIncrement.
	DefaultPoolIncrement = DefaultSessionIncrement
	// DefaultConnectionClass is empty, which allows to use the poolMinSessions created as part of session pool creation for non-DRCP. For DRCP, connectionClass needs to be explicitly mentioned.
	DefaultConnectionClass = dsn.DefaultConnectionClass
	// NoConnectionPoolingConnectionClass is a special connection class name to indicate no connection pooling.
	// It is the same as setting standaloneConnection=1
	NoConnectionPoolingConnectionClass = dsn.NoConnectionPoolingConnectionClass
	// DefaultSessionTimeout is the seconds before idle pool sessions get evicted
	DefaultSessionTimeout = dsn.DefaultSessionTimeout
	// DefaultWaitTimeout is the milliseconds to wait for a session to become available
	DefaultWaitTimeout = dsn.DefaultWaitTimeout
	// DefaultMaxLifeTime is the maximum time in seconds till a pooled session may exist
	DefaultMaxLifeTime = dsn.DefaultMaxLifeTime
	//DefaultStandaloneConnection holds the default for standaloneConnection.
	DefaultStandaloneConnection = dsn.DefaultStandaloneConnection
)

// dsn is separated out for fuzzing, but keep it as "internal"
type (
	ConnectionParams = dsn.ConnectionParams
	CommonParams     = dsn.CommonParams
	ConnParams       = dsn.ConnParams
	PoolParams       = dsn.PoolParams
	Password         = dsn.Password
)

// ParseConnString is deprecated, use ParseDSN.
func ParseConnString(s string) (ConnectionParams, error) { return dsn.Parse(s) }

// ParseDSN parses the given dataSourceName and returns a ConnectionParams structure for use in sql.OpenDB(godror.NewConnector(P)).
func ParseDSN(dataSourceName string) (P ConnectionParams, err error) {
	return dsn.Parse(dataSourceName)
}

func NewPassword(s string) Password { return dsn.NewPassword(s) }

var defaultDrv = &drv{}

func init() {
	sql.Register("godror", defaultDrv)
	// It cannot be longer than 30 bytes !
	if len(DriverName) > 30 {
		DriverName = DriverName[:30]
	}
}

var _ driver.Driver = (*drv)(nil)

type drv struct {
	dpiContext    *C.dpiContext
	pools         map[string]*connPool
	timezones     map[string]locationWithOffSecs
	clientVersion VersionInfo
	mu            sync.RWMutex
}

const oneContext = false

func NewDriver() *drv {
	var d drv
	if !oneContext || defaultDrv == nil {
		return &d
	}
	d.dpiContext = defaultDrv.dpiContext
	return &d
}
func (d *drv) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	dpiCtx, pools := d.dpiContext, d.pools
	d.dpiContext, d.pools, d.timezones = nil, nil, nil
	done := make(chan error, 1)
	go func() {
		for _, pool := range pools {
			pool.Purge()
		}
		done <- nil
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		err = fmt.Errorf("Driver.Close: %w", context.DeadlineExceeded)
	}

	if oneContext &&
		// As we use one global dpiContext, don't destroy it
		(dpiCtx == nil || (d != defaultDrv && defaultDrv.dpiContext == dpiCtx)) {
		return err
	}
	go func() {
		if C.dpiContext_destroy(dpiCtx) == C.DPI_FAILURE {
			done <- fmt.Errorf("error destroying dpiContext %p", dpiCtx)
		}
		close(done)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Driver.Close: %w", context.DeadlineExceeded)
	}
}

type locationWithOffSecs struct {
	*time.Location
	offSecs int
}
type connPool struct {
	dpiPool *C.dpiPool
	key     string
	params  commonAndPoolParams
}

// Purge force-closes the pool's connections then closes the pool.
func (p *connPool) Purge() {
	dpiPool := p.dpiPool
	p.dpiPool = nil
	if dpiPool != nil {
		C.dpiPool_close(dpiPool, C.DPI_MODE_POOL_CLOSE_FORCE)
	}
}

func (p *connPool) Close() error {
	dpiPool := p.dpiPool
	p.dpiPool = nil
	if dpiPool != nil {
		C.dpiPool_release(dpiPool)
	}
	return nil
}

func (d *drv) checkExec(f func() C.int) error {
	runtime.LockOSThread()
	err := d.checkExecNoLOT(f)
	runtime.UnlockOSThread()
	return err
}

func (d *drv) checkExecNoLOT(f func() C.int) error {
	if f() != C.DPI_FAILURE {
		return nil
	}
	return d.getError()
}

func (d *drv) init(configDir, libDir string) error {
	d.mu.RLock()
	ok := d.pools != nil && d.timezones != nil && d.dpiContext != nil
	d.mu.RUnlock()
	if ok {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pools == nil {
		d.pools = make(map[string]*connPool)
	}
	if d.timezones == nil {
		d.timezones = make(map[string]locationWithOffSecs)
	}
	if d.dpiContext != nil {
		return nil
	}
	ctxParams := new(C.dpiContextCreateParams)
	ctxParams.defaultDriverName, ctxParams.defaultEncoding = cDriverName, cUTF8
	if !(configDir == "" && libDir == "") {
		if configDir != "" {
			ctxParams.oracleClientConfigDir = C.CString(configDir)
		}
		if libDir != "" {
			ctxParams.oracleClientLibDir = C.CString(libDir)
		}
	}
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "dpiContext_createWithParams", "params", ctxParams)
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var errInfo C.dpiErrorInfo
	if C.dpiContext_createWithParams(C.uint(DpiMajorVersion), C.uint(DpiMinorVersion),
		ctxParams,
		(**C.dpiContext)(unsafe.Pointer(&d.dpiContext)), &errInfo,
	) == C.DPI_FAILURE {
		return fromErrorInfo(errInfo)
	}

	var v C.dpiVersionInfo
	if C.dpiContext_getClientVersion(d.dpiContext, &v) == C.DPI_FAILURE {
		return fmt.Errorf("getClientVersion: %w", d.getError())
	}
	d.clientVersion.set(&v)
	return nil
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *drv) Open(s string) (driver.Conn, error) {
	c, err := d.OpenConnector(s)
	if err != nil {
		return nil, err
	}
	return d.createConnFromParams(c.(connector).ConnectionParams)
}

func (d *drv) ClientVersion() (VersionInfo, error) {
	return d.clientVersion, nil
}

// UTF-8 is a shortcut name for AL32UTF8 in ODPI-C (and not the same as the botched UTF8).
var cUTF8, cDriverName = C.CString("UTF-8"), C.CString(DriverName)

// initCommonCreateParams initializes ODPI-C common creation parameters used for creating pools and
// standalone connections. The C strings for the encoding and driver name are
// defined at the package level for convenience.
func (d *drv) initCommonCreateParams(P *C.dpiCommonCreateParams, enableEvents bool, stmtCacheSize int, charset string) error {
	// initialize ODPI-C structure for common creation parameters
	if err := d.checkExec(func() C.int {
		return C.dpiContext_initCommonCreateParams(d.dpiContext, P)
	}); err != nil {
		return fmt.Errorf("initCommonCreateParams: %w", err)
	}

	// assign encoding and national encoding
	P.encoding, P.nencoding = cUTF8, cUTF8
	if charset != "" {
		P.encoding = C.CString(charset)
		P.nencoding = P.encoding
	}

	// assign driver name
	P.driverName = cDriverName
	P.driverNameLength = C.uint32_t(len(DriverName))

	// assign creation mode; always use threaded mode in order to allow
	// goroutines to function without mutexing; enable events mode, if
	// requested
	P.createMode = C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED
	if enableEvents {
		P.createMode |= C.DPI_MODE_CREATE_EVENTS
	}
	if stmtCacheSize != 0 {
		if stmtCacheSize < 0 {
			P.stmtCacheSize = 0
		} else {
			P.stmtCacheSize = C.uint32_t(stmtCacheSize)
		}
	}

	return nil
}

// createConn creates an ODPI-C connection with the specified parameters. If a pool is
// provided, the connection is acquired from the pool; otherwise, a standalone
// connection is created.
func (d *drv) createConn(pool *connPool, P commonAndConnParams) (*conn, error) {
	// initialize driver, if necessary
	if err := d.init(P.ConfigDir, P.LibDir); err != nil {
		return nil, err
	}

	dc, err := d.acquireConn(pool, P)
	if err != nil {
		return nil, err
	}
	var poolKey string
	if pool != nil {
		poolKey = pool.key
	}
	// create connection and initialize it, if needed
	c := conn{
		drv: d, dpiConn: dc,
		params:  dsn.ConnectionParams{CommonParams: P.CommonParams, ConnParams: P.ConnParams},
		poolKey: poolKey,
	}
	if pool != nil {
		c.params.PoolParams = pool.params.PoolParams
		if c.params.Username == "" {
			c.params.Username = pool.params.Username
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), nvlD(c.params.WaitTimeout, time.Minute))
	c.init(ctx, getOnInit(&c.params.CommonParams))
	cancel()

	var a [4096]byte
	stack := a[:runtime.Stack(a[:], false)]
	runtime.SetFinalizer(&c, func(c *conn) {
		if c != nil && c.dpiConn != nil {
			fmt.Printf("ERROR: conn %p of createConn is not Closed!\n%s\n", c, stack)
			c.closeNotLocking()
		}
	})
	return &c, nil
}

func (d *drv) acquireConn(pool *connPool, P commonAndConnParams) (*C.dpiConn, error) {
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "acquireConn", "pool", pool, "connParams", P)
	}
	// initialize ODPI-C structure for common creation parameters; this is only
	// used when a standalone connection is being created; when a connection is
	// being acquired from the pool this structure is not needed
	var commonCreateParamsPtr *C.dpiCommonCreateParams
	var commonCreateParams C.dpiCommonCreateParams
	if pool == nil {
		if err := d.initCommonCreateParams(&commonCreateParams, P.EnableEvents, P.StmtCacheSize, P.Charset); err != nil {
			return nil, err
		}
		commonCreateParamsPtr = &commonCreateParams
	}
	// manage strings
	var cUsername, cPassword, cNewPassword, cConnectString, cConnClass *C.char
	defer func() {
		if cUsername != nil {
			C.free(unsafe.Pointer(cUsername))
		}
		if cPassword != nil {
			C.free(unsafe.Pointer(cPassword))
		}
		if cNewPassword != nil {
			C.free(unsafe.Pointer(cNewPassword))
		}
		if cConnectString != nil {
			C.free(unsafe.Pointer(cConnectString))
		}
		if cConnClass != nil {
			C.free(unsafe.Pointer(cConnClass))
		}
	}()

	// initialize ODPI-C structure for connection creation parameters
	var connCreateParams C.dpiConnCreateParams
	if err := d.checkExec(func() C.int {
		return C.dpiContext_initConnCreateParams(d.dpiContext, &connCreateParams)
	}); err != nil {
		return nil, fmt.Errorf("initConnCreateParams: %w", err)
	}

	// assign connection class
	if P.ConnClass != "" {
		cConnClass = C.CString(P.ConnClass)
		connCreateParams.connectionClass = cConnClass
		connCreateParams.connectionClassLength = C.uint32_t(len(P.ConnClass))
	}

	// assign new password (only relevant for standalone connections)
	if pool == nil && !P.NewPassword.IsZero() {
		cNewPassword = C.CString(P.NewPassword.Secret())
		connCreateParams.newPassword = cNewPassword
		connCreateParams.newPasswordLength = C.uint32_t(P.NewPassword.Len())
	}

	// assign external authentication flag (only relevant for standalone
	// connections)
	if pool == nil && P.Username == "" && P.Password.IsZero() {
		connCreateParams.externalAuth = 1
	}

	// assign authorization mode
	connCreateParams.authMode = C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	if P.IsSysDBA {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSDBA
	}
	if P.IsSysOper {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSOPER
	}
	if P.IsSysASM {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSASM
	}
	if P.IsPrelim {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_PRELIM
	}

	// assign sharding keys, if applicable
	if len(P.ShardingKey) > 0 {
		var tempData C.dpiData
		mem := C.malloc(C.sizeof_dpiShardingKeyColumn *
			C.size_t(len(P.ShardingKey)))
		defer C.free(mem)
		columns := (*[(math.MaxInt32 - 1) / C.sizeof_dpiShardingKeyColumn]C.dpiShardingKeyColumn)(mem)
		tbd := make([]func(), 0, len(P.ShardingKey))
		for i, value := range P.ShardingKey {
			switch value := value.(type) {
			case int:
				columns[i].oracleTypeNum = C.DPI_ORACLE_TYPE_NUMBER
				columns[i].nativeTypeNum = C.DPI_NATIVE_TYPE_INT64
				C.dpiData_setInt64(&tempData, C.int64_t(value))
			case string:
				columns[i].oracleTypeNum = C.DPI_ORACLE_TYPE_VARCHAR
				columns[i].nativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
				cs := C.CString(value)
				tbd = append(tbd, func() { C.free(unsafe.Pointer(cs)) })
				C.dpiData_setBytes(&tempData, cs, C.uint32_t(len(value)))
			case []byte:
				columns[i].oracleTypeNum = C.DPI_ORACLE_TYPE_RAW
				columns[i].nativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
				cs := (*C.char)(C.CBytes(value))
				tbd = append(tbd, func() { C.free(unsafe.Pointer(cs)) })
				C.dpiData_setBytes(&tempData, cs, C.uint32_t(len(value)))
			default:
				return nil, errors.New("unsupported data type for sharding")
			}
			columns[i].value = tempData.value
		}
		connCreateParams.shardingKeyColumns = &columns[0]
		connCreateParams.numShardingKeyColumns = C.uint8_t(len(P.ShardingKey))
		if len(tbd) != 0 {
			runtime.SetFinalizer(connCreateParams.shardingKeyColumns,
				func(_ interface{}) {
					for _, f := range tbd {
						f()
					}
				})
		}
	}

	// if a pool was provided, assign the pool
	if pool != nil {
		connCreateParams.pool = pool.dpiPool
	}

	// setup credentials
	username, password := P.Username, P.Password.Secret()
	if pool != nil && !pool.params.Heterogeneous && !pool.params.ExternalAuth {
		// Only for homogeneous pool force user, password as empty.
		username, password = "", ""
	}
	if username != "" {
		cUsername = C.CString(username)
	}
	if password != "" {
		cPassword = C.CString(password)
	}
	if P.ConnectString != "" {
		cConnectString = C.CString(P.ConnectString)
	}

	// create ODPI-C connection
	var dc *C.dpiConn
	if err := d.checkExec(func() C.int {
		return C.dpiConn_create(
			d.dpiContext,
			cUsername, C.uint32_t(len(username)),
			cPassword, C.uint32_t(len(password)),
			cConnectString, C.uint32_t(len(P.ConnectString)),
			commonCreateParamsPtr,
			&connCreateParams, &dc,
		)
	}); err != nil {
		if pool != nil {
			stats, _ := d.getPoolStats(pool)
			return nil, fmt.Errorf("pool=%p stats=%s params=%+v: %w",
				pool.dpiPool, stats, connCreateParams, err)
		}
		return nil, fmt.Errorf("user=%q standalone params=%+v: %w",
			username, connCreateParams, err)
	}
	return dc, nil
}

// createConnFromParams creates a driver connection given pool parameters and connection
// parameters. The pool parameters are used to either create a pool or use an
// existing cached pool.
//
// If the pool parameters are nil, no pool is used and a
// standalone connection is created instead. The connection parameters are used
// to acquire a connection from the pool specified by the pool parameters or
// are used to create a standalone connection.
func (d *drv) createConnFromParams(P dsn.ConnectionParams) (*conn, error) {
	var err error
	var pool *connPool
	if !P.IsStandalone() {
		pool, err = d.getPool(commonAndPoolParams{CommonParams: P.CommonParams, PoolParams: P.PoolParams})
		if err != nil {
			return nil, err
		}
	}
	conn, err := d.createConn(pool, commonAndConnParams{CommonParams: P.CommonParams, ConnParams: P.ConnParams})
	if err != nil {
		return conn, err
	}
	onInit := getOnInit(&conn.params.CommonParams)
	if onInit == nil {
		return conn, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), nvlD(conn.params.WaitTimeout, time.Minute))
	err = onInit(ctx, conn)
	cancel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// getPool get the pool to use given the set of pool parameters provided.
//
// Pools are stored in a map keyed by a string representation of the pool parameters.
// If no pool exists, a pool is created and stored in the map.
func (d *drv) getPool(P commonAndPoolParams) (*connPool, error) {
	// initialize driver, if necessary
	if err := d.init(P.ConfigDir, P.LibDir); err != nil {
		return nil, err
	}

	var usernameKey string
	if !P.Heterogeneous && !P.ExternalAuth {
		// skip username being part of key in heterogeneous pools
		usernameKey = P.Username
	}
	// determine key to use for pool
	poolKey := fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%t\t%t\t%t\t%s\t%d\t%s",
		usernameKey, P.ConnectString, P.MinSessions, P.MaxSessions,
		P.SessionIncrement, P.WaitTimeout, P.MaxLifeTime, P.SessionTimeout,
		P.Heterogeneous, P.EnableEvents, P.ExternalAuth,
		P.Timezone, P.MaxSessionsPerShard, P.PingInterval,
	)
	logger := getLogger()
	if logger != nil {
		logger.Log("msg", "getPool", "key", poolKey)
	}

	// if pool already exists, return it immediately; otherwise, create a new
	// pool; hold the lock while the pool is looked up (and created, if needed)
	// in order to ensure that multiple goroutines do not attempt to create a
	// pool
	d.mu.RLock()
	pool, ok := d.pools[poolKey]
	d.mu.RUnlock()
	if ok {
		return pool, nil
	}
	// createPool uses checkExec wich needs getError which uses RLock,
	// so we cannot Lock here, thus this little race window for
	// creating a pool and throwing it away.
	pool, err := d.createPool(P)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if poolOld, ok := d.pools[poolKey]; ok {
		_ = pool.Close()
		return poolOld, nil
	}
	pool.key = poolKey
	d.pools[poolKey] = pool
	return pool, nil
}

// createPool creates an ODPI-C pool with the specified parameters.
//
// This is done while holding the mutex in order to ensure that
// multiple goroutines do not attempt to create the pool at the same time.
func (d *drv) createPool(P commonAndPoolParams) (*connPool, error) {

	// set up common creation parameters
	var commonCreateParams C.dpiCommonCreateParams
	if err := d.initCommonCreateParams(&commonCreateParams, P.EnableEvents, P.StmtCacheSize, P.Charset); err != nil {
		return nil, err
	}

	// initialize ODPI-C structure for pool creation parameters
	var poolCreateParams C.dpiPoolCreateParams
	if err := d.checkExec(func() C.int {
		return C.dpiContext_initPoolCreateParams(d.dpiContext, &poolCreateParams)
	}); err != nil {
		return nil, fmt.Errorf("initPoolCreateParams: %w", err)
	}

	// assign minimum number of sessions permitted in the pool
	poolCreateParams.minSessions = dsn.DefaultPoolMinSessions
	if P.MinSessions >= 0 {
		poolCreateParams.minSessions = C.uint32_t(P.MinSessions)
	}

	// assign maximum number of sessions permitted in the pool
	poolCreateParams.maxSessions = dsn.DefaultPoolMaxSessions
	if P.MaxSessions > 0 {
		poolCreateParams.maxSessions = C.uint32_t(P.MaxSessions)
	}

	// assign the number of sessions to create each time more is needed
	poolCreateParams.sessionIncrement = dsn.DefaultPoolIncrement
	if P.SessionIncrement > 0 {
		poolCreateParams.sessionIncrement = C.uint32_t(P.SessionIncrement)
	}

	// assign "get" mode (always used timed wait)
	poolCreateParams.getMode = C.DPI_MODE_POOL_GET_TIMEDWAIT

	// assign wait timeout (number of milliseconds to wait for a session to
	// become available
	poolCreateParams.waitTimeout = C.uint32_t(dsn.DefaultWaitTimeout / time.Millisecond)
	if P.WaitTimeout > 0 {
		poolCreateParams.waitTimeout = C.uint32_t(P.WaitTimeout / time.Millisecond)
	}

	// assign timeout (number of seconds before idle pool session are evicted
	// from the pool
	poolCreateParams.timeout = C.uint32_t(dsn.DefaultSessionTimeout / time.Second)
	if P.SessionTimeout > 0 {
		poolCreateParams.timeout = C.uint32_t(P.SessionTimeout / time.Second)
	}

	// assign maximum lifetime (number of seconds a pooled session may exist)
	poolCreateParams.maxLifetimeSession = C.uint32_t(dsn.DefaultMaxLifeTime / time.Second)
	if P.MaxLifeTime > 0 {
		poolCreateParams.maxLifetimeSession = C.uint32_t(P.MaxLifeTime / time.Second)
	}

	// assign external authentication flag
	poolCreateParams.externalAuth = C.int(b2i(P.ExternalAuth))

	// assign homogeneous pool flag; default is true so need to clear the flag
	// if specifically reqeuested or if external authentication is desirable
	if poolCreateParams.externalAuth == 1 || P.Heterogeneous {
		poolCreateParams.homogeneous = 0
	}

	// setup credentials
	var cUsername, cPassword, cConnectString *C.char
	if P.Username != "" {
		cUsername = C.CString(P.Username)
		defer C.free(unsafe.Pointer(cUsername))
	}
	if !P.Password.IsZero() {
		cPassword = C.CString(P.Password.Secret())
		defer C.free(unsafe.Pointer(cPassword))
	}
	if P.ConnectString != "" {
		cConnectString = C.CString(P.ConnectString)
		defer C.free(unsafe.Pointer(cConnectString))
	}

	// create pool
	var dp *C.dpiPool
	logger := getLogger()
	if logger != nil {
		logger.Log("C", "dpiPool_create", "user", P.Username, "ConnectString", P.ConnectString,
			"common", commonCreateParams, "pool",
			fmt.Sprintf("%#v", poolCreateParams))
	}
	if err := d.checkExec(func() C.int {
		return C.dpiPool_create(
			d.dpiContext,
			cUsername, C.uint32_t(len(P.Username)),
			cPassword, C.uint32_t(P.Password.Len()),
			cConnectString, C.uint32_t(len(P.ConnectString)),
			&commonCreateParams,
			&poolCreateParams,
			(**C.dpiPool)(unsafe.Pointer(&dp)),
		)
	}); err != nil {
		return nil, fmt.Errorf("dpoPool_create user=%s extAuth=%v: %w",
			P.Username, poolCreateParams.externalAuth, err)
	}

	// set statement cache
	stmtCacheSize := C.uint32_t(40)
	if P.StmtCacheSize != 0 {
		if P.StmtCacheSize < 0 {
			stmtCacheSize = 0
		} else {
			stmtCacheSize = C.uint32_t(P.StmtCacheSize)
		}
	}
	C.dpiPool_setStmtCacheSize(dp, stmtCacheSize)

	return &connPool{dpiPool: dp, params: P}, nil
}

// PoolStats contains Oracle session pool statistics
type PoolStats struct {
	Busy, Open, Max                   uint32
	MaxLifetime, Timeout, WaitTimeout time.Duration
}

func (s PoolStats) String() string {
	return fmt.Sprintf("busy=%d open=%d max=%d maxLifetime=%s timeout=%s waitTimeout=%s",
		s.Busy, s.Open, s.Max, s.MaxLifetime, s.Timeout, s.WaitTimeout)
}

// Stats returns PoolStats of the pool.
func (d *drv) getPoolStats(p *connPool) (stats PoolStats, err error) {
	if p == nil || p.dpiPool == nil {
		return stats, nil
	}

	stats.Max = uint32(p.params.PoolParams.MaxSessions)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var u C.uint32_t
	if C.dpiPool_getBusyCount(p.dpiPool, &u) != C.DPI_FAILURE {
		stats.Busy = uint32(u)
	}
	if C.dpiPool_getOpenCount(p.dpiPool, &u) != C.DPI_FAILURE {
		stats.Open = uint32(u)
	}
	if C.dpiPool_getMaxLifetimeSession(p.dpiPool, &u) != C.DPI_FAILURE {
		stats.MaxLifetime = time.Duration(u) * time.Second
	}
	if C.dpiPool_getTimeout(p.dpiPool, &u) != C.DPI_FAILURE {
		stats.Timeout = time.Duration(u) * time.Second
	}
	if C.dpiPool_getWaitTimeout(p.dpiPool, &u) != C.DPI_FAILURE {
		stats.WaitTimeout = time.Duration(u) * time.Millisecond
		return stats, nil
	}
	return stats, d.getError()
}

type commonAndConnParams struct {
	dsn.CommonParams
	dsn.ConnParams
}

func (P commonAndConnParams) String() string {
	return P.CommonParams.String() + " " + P.ConnParams.String()
}

type commonAndPoolParams struct {
	dsn.CommonParams
	dsn.PoolParams
}

func (P commonAndPoolParams) String() string {
	return P.CommonParams.String() + " " + P.PoolParams.String()
}

// OraErr is an error holding the ORA-01234 code and the message.
type OraErr struct {
	message, funName, action, sqlState string
	code, offset                       int
	recoverable, warning               bool
}

// AsOraErr returns the underlying *OraErr and whether it succeeded.
func AsOraErr(err error) (*OraErr, bool) {
	var oerr *OraErr
	ok := errors.As(err, &oerr)
	return oerr, ok
}

var _ error = (*OraErr)(nil)

// Code returns the OraErr's error code.
func (oe *OraErr) Code() int {
	if oe == nil {
		return 0
	}
	return oe.code
}

// Message returns the OraErr's message.
func (oe *OraErr) Message() string {
	if oe == nil {
		return ""
	}
	return oe.message
}
func (oe *OraErr) Error() string {
	if oe == nil {
		return ""
	}
	msg := oe.Message()
	if oe.code == 0 && msg == "" {
		return ""
	}
	return fmt.Sprintf("ORA-%05d: %s", oe.code, oe.message)
}
func fromErrorInfo(errInfo C.dpiErrorInfo) error {
	oe := OraErr{
		code:        int(errInfo.code),
		message:     strings.TrimSpace(C.GoStringN(errInfo.message, C.int(errInfo.messageLength))),
		offset:      int(errInfo.offset),
		funName:     strings.TrimSpace(C.GoString(errInfo.fnName)),
		action:      strings.TrimSpace(C.GoString(errInfo.action)),
		sqlState:    strings.TrimSpace(C.GoString(errInfo.sqlState)),
		recoverable: errInfo.isRecoverable != 0,
		warning:     errInfo.isWarning != 0,
	}
	if oe.code == 0 && oe.message == "" {
		return nil
	}
	if oe.code == 0 && strings.HasPrefix(oe.message, "ORA-") &&
		len(oe.message) > 9 && oe.message[9] == ':' {
		if i, _ := strconv.Atoi(oe.message[4:9]); i > 0 {
			oe.code, oe.message = i, strings.TrimSpace(oe.message[10:])
		}
	}
	oe.message = strings.TrimPrefix(oe.message, fmt.Sprintf("ORA-%05d: ", oe.Code()))
	return &oe
}

// Offset returns the parse error offset (in bytes) when executing a statement or the row offset when performing bulk operations or fetching batch error information. If neither of these cases are true, the value is 0.
func (oe *OraErr) Offset() int { return oe.offset }

// FunName returns the public ODPI-C function name which was called in which the error took place. This is a null-terminated ASCII string.
func (oe *OraErr) FunName() string { return oe.funName }

// Action returns the internal action that was being performed when the error took place. This is a null-terminated ASCII string.
func (oe *OraErr) Action() string { return oe.action }

// SQLState returns the SQLSTATE code associated with the error. This is a 5 character null-terminated string.
func (oe *OraErr) SQLState() string { return oe.sqlState }

// Recoverable indicates if the error is recoverable. This is always false unless both client and server are at release 12.1 or higher.
func (oe *OraErr) Recoverable() bool { return oe.recoverable }

func (oe *OraErr) IsWarning() bool { return oe.warning }

// newErrorInfo is just for testing: testing cannot use Cgo...
func newErrorInfo(code int, message string) C.dpiErrorInfo {
	return C.dpiErrorInfo{code: C.int32_t(code), message: C.CString(message), messageLength: C.uint(len(message))}
}

// against deadcode
var _ = newErrorInfo

func (d *drv) getError() error {
	if d == nil {
		return &OraErr{code: 12153, message: "getError on nil drv: " + driver.ErrBadConn.Error()}
	}
	d.mu.RLock()
	dpiContext := d.dpiContext
	d.mu.RUnlock()

	if dpiContext == nil {
		return &OraErr{code: 12153, message: "getError on in dpiContext: " + driver.ErrBadConn.Error()}
	}
	var errInfo C.dpiErrorInfo
	C.dpiContext_getError(dpiContext, &errInfo)
	return fromErrorInfo(errInfo)
}
func b2i(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// VersionInfo holds version info returned by Oracle DB.
type VersionInfo struct {
	ServerRelease                                           string
	Version, Release, Update, PortRelease, PortUpdate, Full uint8
}

func (V *VersionInfo) set(v *C.dpiVersionInfo) {
	*V = VersionInfo{
		Version: uint8(v.versionNum),
		Release: uint8(v.releaseNum), Update: uint8(v.updateNum),
		PortRelease: uint8(v.portReleaseNum), PortUpdate: uint8(v.portUpdateNum),
		Full: uint8(v.fullVersionNum),
	}
}
func (V *VersionInfo) String() string {
	var s string
	if V.ServerRelease != "" {
		s = " [" + V.ServerRelease + "]"
	}
	return fmt.Sprintf("%d.%d.%d.%d.%d%s", V.Version, V.Release, V.Update, V.PortRelease, V.PortUpdate, s)
}

var timezones = make(map[string]*time.Location)
var timezonesMu sync.RWMutex

func timeZoneFor(hourOffset, minuteOffset C.int8_t, local *time.Location) *time.Location {
	if hourOffset == 0 && minuteOffset == 0 {
		if local == nil {
			return time.UTC
		}
		return local
	}
	var tS string
	if local != nil {
		tS = local.String()
	}
	key := fmt.Sprintf("%02d:%02d\t%s", hourOffset, minuteOffset, tS)
	timezonesMu.RLock()
	tz, ok := timezones[key]
	timezonesMu.RUnlock()
	if ok {
		return tz
	}
	timezonesMu.Lock()
	defer timezonesMu.Unlock()
	if tz, ok = timezones[key]; ok {
		return tz
	}
	off := int(hourOffset)*3600 + int(minuteOffset)*60
	if local != nil {
		if _, localOff := time.Now().In(local).Zone(); off == localOff {
			tz = local
		}
	}
	if tz == nil {
		tz = time.FixedZone(key[:5], off)
	}
	timezones[key] = tz
	return tz
}

var _ driver.DriverContext = (*drv)(nil)
var _ driver.Connector = (*connector)(nil)
var _ io.Closer = (*connector)(nil)

type connector struct {
	drv *drv
	dsn.ConnectionParams
}

// NewConnector returns a driver.Connector to be used with sql.OpenDB
//
// ConnectionParams must be complete, so start with what ParseDSN returns!
func (d *drv) NewConnector(params dsn.ConnectionParams) driver.Connector {
	return connector{drv: d, ConnectionParams: params}
}

// NewConnector returns a driver.Connector to be used with sql.OpenDB,
// (for the default Driver registered with godror)
//
// ConnectionParams must be complete, so start with what ParseDSN returns!
func NewConnector(params dsn.ConnectionParams) driver.Connector {
	return defaultDrv.NewConnector(params)
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *drv) OpenConnector(name string) (driver.Connector, error) {

	// parse connection string
	P, err := dsn.Parse(name)
	if err != nil {
		return nil, err
	}

	return NewConnector(P), nil
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes.
//
// The returned connection is only used by one goroutine at a
// time.
func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	params := c.ConnectionParams
	logger := ctxGetLog(ctx)
	if ctxValue := ctx.Value(paramsCtxKey{}); ctxValue != nil {
		if cc, ok := ctxValue.(commonAndConnParams); ok {
			// ContextWithUserPassw does not fill ConnParam.ConnectString
			if cc.ConnectString == "" {
				cc.ConnectString = params.ConnectString
			}
			if logger != nil {
				logger.Log("msg", "connect with params from context", "poolParams", params.PoolParams, "connParams", cc, "common", cc.CommonParams)
			}
			return c.drv.createConnFromParams(dsn.ConnectionParams{
				CommonParams: cc.CommonParams, ConnParams: cc.ConnParams,
				PoolParams: params.PoolParams,
			})
		}
	}

	if ctxValue := ctx.Value(userPasswCtxKey{}); ctxValue != nil {
		if up, ok := ctxValue.(UserPasswdConnClassTag); ok {
			params.CommonParams.Username = up.Username
			params.CommonParams.Password = up.Password
			params.ConnParams.ConnClass = up.ConnClass
		}
	}

	if logger != nil {
		logger.Log("msg", "connect", "poolParams", params.PoolParams, "connParams", params.ConnParams, "common", params.CommonParams)
	}
	return c.drv.createConnFromParams(params)
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c connector) Driver() driver.Driver { return c.drv }

// Close the connector's underlying driver.
//
// From Go 1.17 sql.DB.Close() will call this method.
func (c connector) Close() error {
	if c.drv == nil || oneContext || c.drv == defaultDrv {
		return nil
	}
	return c.drv.Close()
}

// NewSessionIniter returns a function suitable for use in NewConnector as onInit,
//
// Deprecated. Use ParseDSN + ConnectionParams.SetSessionParamOnInit and NewConnector.
// which calls "ALTER SESSION SET <key>='<value>'" for each element of the given map.
func NewSessionIniter(m map[string]string) func(context.Context, driver.ConnPrepareContext) error {
	var buf strings.Builder
	buf.WriteString("ALTER SESSION SET ")
	for k, v := range m {
		buf.WriteByte(' ')
		fmt.Fprintf(&buf, "%s=q'(%s)'", k, strings.Replace(v, "'", "''", -1))
	}
	return mkExecMany([]string{buf.String()})
}
func getOnInit(P *CommonParams) func(context.Context, driver.ConnPrepareContext) error {
	if P.OnInit != nil {
		return P.OnInit
	}
	stmts := P.OnInitStmts
	stmts = stmts[:len(stmts):len(stmts)]
	if len(P.AlterSession) != 0 {
		var buf strings.Builder
		buf.WriteString("ALTER SESSION SET")
		for _, kv := range P.AlterSession {
			buf.WriteByte(' ')
			buf.WriteString(kv[0])
			buf.WriteByte('=')
			if strings.EqualFold(kv[0], "CURRENT_SCHEMA") {
				buf.WriteString(kv[1])
			} else {
				buf.WriteByte('\'')
				buf.WriteString(strings.Replace(kv[1], "'", "''", -1))
				buf.WriteByte('\'')
			}
		}
		stmts = append(stmts, buf.String())
	}
	if len(stmts) == 0 {
		return nil
	}

	P.OnInit = mkExecMany(stmts)
	return P.OnInit
}

// mkExecMany returns a function that applies the queries to the connection.
func mkExecMany(qrys []string) func(context.Context, driver.ConnPrepareContext) error {
	return func(ctx context.Context, conn driver.ConnPrepareContext) error {
		logger := ctxGetLog(ctx)
		for _, qry := range qrys {
			if logger != nil {
				logger.Log("msg", "execMany", "qry", qry)
			}
			st, err := conn.PrepareContext(ctx, qry)
			if err == nil {
				_, err = st.(driver.StmtExecContext).ExecContext(ctx, nil)
				st.Close()
			}
			if err != nil {
				return fmt.Errorf("%s: %w", qry, err)
			}
		}
		return nil
	}
}

func nvlD(a, b time.Duration) time.Duration {
	if a == 0 {
		return b
	}
	return a
}
