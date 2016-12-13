package base

import (
	"fmt"
	"strings"

	"udup/client/driver/mysql/binlog"
	"udup/client/driver/mysql/sql"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

func (c *ConnectionConfig) Duplicate() *ConnectionConfig {
	config := &ConnectionConfig{
		Host:     c.Host,
		Port:     c.Port,
		User:     c.User,
		Password: c.Password,
	}
	return config
}

func (c *ConnectionConfig) GetDBUri() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8,latin1", c.User, c.Password, c.Host, c.Port)
}

type MySQLContext struct {
	defaultNumRetries int64

	NullableUniqueKeyAllowed bool

	KafkaClusterAddr         string
	NatsServerAddr           string
	ServerId                 uint32
	Database                 []string
	Table                    []sql.TableName
	HasSuperPrivilege        bool
	OriginalBinlogFormat     string
	OriginalBinlogRowImage   string
	ConnectionConfig         *ConnectionConfig
	InitialBinlogCoordinates *binlog.BinlogCoordinates

	//validateTable
	TableEngine string
}

var context *MySQLContext

func init() {
	context = newMySQLContext()
}

func newMySQLContext() *MySQLContext {
	return &MySQLContext{
		defaultNumRetries: 60,
	}
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (m *MySQLContext) RequiresBinlogFormatChange() bool {
	return m.OriginalBinlogFormat != "ROW"
}

func (this *MySQLContext) MaxRetries() int64 {
	retries := this.defaultNumRetries
	return retries
}

func (m *MySQLContext) IsTransactionalTable() bool {
	switch strings.ToLower(m.TableEngine) {
	case "innodb":
		{
			return true
		}
	case "tokudb":
		{
			return true
		}
	}
	return false
}
