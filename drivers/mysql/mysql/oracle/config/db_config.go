package config

import (
	"bytes"
	"context"
	"database/sql"
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

type OracleDB struct {
	_db          *sql.DB
	LogMinerConn *sql.Conn
	MetaDataConn *sql.Conn
}

func (m *OracleConfig) ConnectString() string {
	return fmt.Sprintf("%s:%d/%s", m.Host, m.Port, m.ServiceName)
}

func NewDB(meta *OracleConfig) (*OracleDB, error) {
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
	oracleDB := &OracleDB{_db: sqlDB}

	oracleDB.LogMinerConn, err = sqlDB.Conn(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error on get connection:%v", err)
	}
	oracleDB.MetaDataConn, err = sqlDB.Conn(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error on get connection:%v", err)
	}
	return oracleDB, nil
}

func (o *OracleDB) CurrentRedoLogSequenceFp() (string, error) {
	query := fmt.Sprintf(`SELECT GROUP#, THREAD#, SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'`)
	rows, err := o.LogMinerConn.QueryContext(context.TODO(), query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	buf := bytes.Buffer{}
	for rows.Next() {
		var group string
		var thread string
		var sequence string
		err = rows.Scan(&group, &thread, &sequence)
		if err != nil {
			return "", err
		}
		buf.WriteString(fmt.Sprintf("group:%s,thread:%s,sequence:%s",
			group, thread, sequence))
		buf.WriteString(";")
	}
	return buf.String(), nil
}

func (o *OracleDB) GetTableDDL(schema, table string) (string, error) {
	ctx := context.TODO()
	o.MetaDataConn.ExecContext(ctx, `begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false); end;`)
	o.MetaDataConn.ExecContext(ctx, `begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false); end;`)
	o.MetaDataConn.ExecContext(ctx, `begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true); end;`)
	row := o.MetaDataConn.QueryRowContext(ctx, fmt.Sprintf(`
SELECT dbms_metadata.get_ddl('TABLE','%s','%s') FROM dual`, table, schema))
	var query string
	err := row.Scan(&query)
	if err != nil {
		return "", err
	}
	return query, nil
}
