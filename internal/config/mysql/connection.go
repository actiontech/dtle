package mysql

import (
	"fmt"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

func (c *ConnectionConfig) GetDBUriByDbName(databaseName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4,utf8,latin1", c.User, c.Password, c.Host, c.Port, databaseName)
}

func (c *ConnectionConfig) GetDBUri() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&tls=false&autocommit=true&charset=utf8mb4,utf8,latin1&multiStatements=true", c.User, c.Password, c.Host, c.Port)
}
