package mysql

import (
	"fmt"
)

// ConnectionConfig is the minimal configuration required to connect to a MySQL server
type ConnectionConfig struct {
	Key      InstanceKey
	User     string
	Password string
}

func NewConnectionConfig() *ConnectionConfig {
	config := &ConnectionConfig{
		Key: InstanceKey{},
	}
	return config
}

// DuplicateCredentials creates a new connection config with given key and with same credentials as this config
func (c *ConnectionConfig) DuplicateCredentials(key InstanceKey) *ConnectionConfig {
	config := &ConnectionConfig{
		Key:      key,
		User:     c.User,
		Password: c.Password,
	}
	return config
}

func (c *ConnectionConfig) Duplicate() *ConnectionConfig {
	return c.DuplicateCredentials(c.Key)
}

func (c *ConnectionConfig) String() string {
	return fmt.Sprintf("%s, user=%s", c.Key.DisplayString(), c.User)
}

/*func (c *ConnectionConfig) Equals(other *ConnectionConfig) bool {
	return c.Key.Equals(&other.Key) || c.ImpliedKey.Equals(other.ImpliedKey)
}*/

func (c *ConnectionConfig) GetDBUriByDbName(databaseName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4,utf8,latin1", c.User, c.Password, c.Key.Host, c.Key.Port, databaseName)
}

func (c *ConnectionConfig) GetDBUri() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&tls=false&autocommit=true&charset=utf8mb4,utf8,latin1&multiStatements=true", c.User, c.Password, c.Key.Host, c.Key.Port)
}
