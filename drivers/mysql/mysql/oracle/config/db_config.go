package config

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	_ "github.com/sijms/go-ora/v2"
)

type OracleConfig struct {
	User        string
	Password    string
	Host        string
	Port        int
	ServiceName string
	Scn         int64
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
	if meta.ServiceName == "" {
		meta.ServiceName = "xe"
	}
	sqlDB, err := sql.Open("oracle", fmt.Sprintf("oracle://%s:%s@%s:%d/%s", meta.User, meta.Password, meta.Host, meta.Port, meta.ServiceName))
	if err != nil {
		return nil, fmt.Errorf("error on open oracle database connection:%v", err)
	}
	// oraDsn := godror.ConnectionParams{
	// 	CommonParams: godror.CommonParams{
	// 		Username:      meta.User,
	// 		ConnectString: meta.ConnectString(),
	// 		Password:      godror.NewPassword(meta.Password),
	// 	},
	// 	PoolParams: godror.PoolParams{
	// 		MinSessions:    dsn.DefaultPoolMinSessions,
	// 		MaxSessions:    dsn.DefaultPoolMaxSessions,
	// 		WaitTimeout:    dsn.DefaultWaitTimeout,
	// 		MaxLifeTime:    dsn.DefaultMaxLifeTime,
	// 		SessionTimeout: dsn.DefaultSessionTimeout,
	// 	},
	// }
	// sqlDB := sql.OpenDB(godror.NewConnector(oraDsn))

	err = sqlDB.Ping()
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

func (o *OracleDB) Close() error {
	if o.MetaDataConn != nil {
		o.MetaDataConn.Close()
	}
	if o.LogMinerConn != nil {
		o.LogMinerConn.Close()
	}
	return o._db.Close()
}

func (o *OracleDB) CurrentRedoLogSequenceFp() (string, error) {
	query := `SELECT GROUP#, THREAD#, SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'`
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

// reset date/timestamp format
func (o *OracleDB) NLS_DATE_FORMAT() error {
	SQL_ALTER_DATE_FORMAT := `ALTER SESSION SET NLS_DATE_FORMAT = 'SYYYY-MM-DD HH24:MI:SS'`
	_, err := o.LogMinerConn.ExecContext(context.TODO(), SQL_ALTER_DATE_FORMAT)
	if err != nil {
		return err
	}
	NLS_TIMESTAMP_FORMAT := "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF6'"
	_, err = o.LogMinerConn.ExecContext(context.TODO(), NLS_TIMESTAMP_FORMAT)
	if err != nil {
		return err
	}
	return nil
}
func (o *OracleDB) GetTables(schema string) ([]string, error) {
	query := fmt.Sprintf(`
SELECT 
	table_name
FROM 
	all_tables 
WHERE 
	owner = '%s'`, schema)

	rows, err := o.MetaDataConn.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (o *OracleDB) GetSchemas() ([]string, error) {
	query := `SELECT
	USERNAME
	FROM
	DBA_USERS
	WHERE
	USERNAME NOT IN ( 'SYS', 'SYSTEM', 'ANONYMOUS', 'APEX_PUBLIC_USER', 'APEX_040000', 'OUTLN', 'XS$NULL', 'FLOWS_FILES', 'MDSYS', 'CTXSYS', 'XDB', 'HR' )`

	rows, err := o.MetaDataConn.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (o *OracleDB) GetColumns(schema, table string) ([]string, error) {
	query := fmt.Sprintf(`SELECT column_name
	FROM all_tab_cols
	WHERE table_name = '%s'
	AND owner = '%s'
	ORDER BY COLUMN_ID`, table, schema)

	rows, err := o.MetaDataConn.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
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
