// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package conn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/go-sql-driver/mysql"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
)

var customID int64

// DBProvider providers BaseDB instance.
type DBProvider interface {
	Apply(config config.DBConfig) (*BaseDB, error)
}

// DefaultDBProviderImpl is default DBProvider implement.
type DefaultDBProviderImpl struct{}

// DefaultDBProvider is global instance of DBProvider.
var DefaultDBProvider DBProvider

func init() {
	DefaultDBProvider = &DefaultDBProviderImpl{}
}

// mockDB is used in unit test.
var mockDB sqlmock.Sqlmock

// Apply will build BaseDB with DBConfig.
func (d *DefaultDBProviderImpl) Apply(config config.DBConfig) (*BaseDB, error) {
	// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
	// https://github.com/go-sql-driver/mysql#maxallowedpacket
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=0",
		config.User, config.Password, config.Host, config.Port)

	doFuncInClose := func() {}
	if config.Security != nil {
		if loadErr := config.Security.LoadTLSContent(); loadErr != nil {
			return nil, terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err := toolutils.ToTLSConfigWithVerifyByRawbytes(config.Security.SSLCABytes,
			config.Security.SSLCertBytes, config.Security.SSLKEYBytes, config.Security.CertAllowedCN)
		if err != nil {
			return nil, terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
		// NOTE for local test(use a self-signed or invalid certificate), we don't need to check CA file.
		// see more here https://github.com/go-sql-driver/mysql#tls
		if config.Host == "127.0.0.1" {
			tlsConfig.InsecureSkipVerify = true
		}

		name := "dm" + strconv.FormatInt(atomic.AddInt64(&customID, 1), 10)
		err = mysql.RegisterTLSConfig(name, tlsConfig)
		if err != nil {
			return nil, terror.ErrConnRegistryTLSConfig.Delegate(err)
		}
		dsn += "&tls=" + name

		doFuncInClose = func() {
			mysql.DeregisterTLSConfig(name)
		}
	}

	var maxIdleConns int
	rawCfg := config.RawDBCfg
	if rawCfg != nil {
		if rawCfg.ReadTimeout != "" {
			dsn += fmt.Sprintf("&readTimeout=%s", rawCfg.ReadTimeout)
		}
		if rawCfg.WriteTimeout != "" {
			dsn += fmt.Sprintf("&writeTimeout=%s", rawCfg.WriteTimeout)
		}
		maxIdleConns = rawCfg.MaxIdleConns
	}

	for key, val := range config.Session {
		// for num such as 1/"1", format as key='1'
		// for string, format as key='string'
		// both are valid for mysql and tidb
		dsn += fmt.Sprintf("&%s='%s'", key, url.QueryEscape(val))
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	failpoint.Inject("failDBPing", func(_ failpoint.Value) {
		db.Close()
		db, mockDB, _ = sqlmock.New()
		mockDB.ExpectPing()
		mockDB.ExpectClose()
	})

	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	defer cancel()
	err = db.PingContext(ctx)
	failpoint.Inject("failDBPing", func(_ failpoint.Value) {
		err = errors.New("injected error")
	})
	if err != nil {
		db.Close()
		doFuncInClose()
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	db.SetMaxIdleConns(maxIdleConns)

	return NewBaseDB(db, doFuncInClose), nil
}

// BaseDB wraps *sql.DB, control the BaseConn.
type BaseDB struct {
	DB *sql.DB

	mu sync.Mutex // protects following fields
	// hold all db connections generated from this BaseDB
	conns map[*BaseConn]struct{}

	Retry retry.Strategy

	// this function will do when close the BaseDB
	doFuncInClose func()
}

// NewBaseDB returns *BaseDB object.
func NewBaseDB(db *sql.DB, doFuncInClose func()) *BaseDB {
	conns := make(map[*BaseConn]struct{})
	return &BaseDB{DB: db, conns: conns, Retry: &retry.FiniteRetryStrategy{}, doFuncInClose: doFuncInClose}
}

// GetBaseConn retrieves *BaseConn which has own retryStrategy.
func (d *BaseDB) GetBaseConn(ctx context.Context) (*BaseConn, error) {
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	baseConn := NewBaseConn(conn, d.Retry)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[baseConn] = struct{}{}
	return baseConn, nil
}

// CloseBaseConn release BaseConn resource from BaseDB, and close BaseConn.
func (d *BaseDB) CloseBaseConn(conn *BaseConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, conn)
	return conn.close()
}

// Close release *BaseDB resource.
func (d *BaseDB) Close() error {
	if d == nil || d.DB == nil {
		return nil
	}
	var err error
	d.mu.Lock()
	defer d.mu.Unlock()
	for conn := range d.conns {
		terr := conn.close()
		if err == nil {
			err = terr
		}
	}
	terr := d.DB.Close()
	d.doFuncInClose()

	if err == nil {
		return terr
	}

	return err
}
