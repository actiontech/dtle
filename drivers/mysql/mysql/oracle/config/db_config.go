package config

import (
	"database/sql"
	gosql "database/sql"
	"fmt"

	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

type OracleConfig struct {
	User        string
	Password    string
	Host        string
	Port        uint32
	ServiceName string
	SCN         int64
}

func (m *OracleConfig) ConnectString() string {
	return fmt.Sprintf("%s:%d/%s", m.Host, m.Port, m.ServiceName)
}

func NewDB(meta *OracleConfig) (*gosql.DB, error) {
	oraDsn := godror.ConnectionParams{
		CommonParams: godror.CommonParams{
			Username:      meta.User,
			ConnectString: meta.ConnectString(),
			Password:      godror.NewPassword(meta.Password),
		},
		PoolParams: godror.PoolParams{
			MinSessions:    dsn.DefaultPoolMinSessions,
			MaxSessions:    dsn.DefaultPoolMaxSessions,
			WaitTimeout:    dsn.DefaultWaitTimeout,
			MaxLifeTime:    dsn.DefaultMaxLifeTime,
			SessionTimeout: dsn.DefaultSessionTimeout,
		},
	}
	sqlDB := sql.OpenDB(godror.NewConnector(oraDsn))

	err := sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return sqlDB, nil
}
