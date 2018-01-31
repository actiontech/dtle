package driver

import (
	"udup/internal/models"
	"github.com/mitchellh/mapstructure"
	"fmt"
	"udup/internal/config"
	"strings"
	"udup/internal/client/driver/kafka"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/client/driver/mysql/base"
)

type KafkaDriver struct {
	DriverContext
}

func NewKafkaDriver(ctx *DriverContext) Driver {
	return &KafkaDriver{DriverContext: *ctx}
}

func (k *KafkaDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	// TODO: use kafka config only
	var mysqlConfig config.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &mysqlConfig); err != nil {
		return nil, err
	}
	var kafkaConfig config.KafkaDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &kafkaConfig); err != nil {
		return nil, err
	}
	c := kafka.NewConnector(ctx.Subject, &mysqlConfig, &kafkaConfig, k.logger)
	go c.Run()
	return c, nil
}

// Validate is used to validate the driver configuration
func (m *KafkaDriver) Validate(task *models.Task) (*models.TaskValidateResponse, error) {
	var driverConfig config.MySQLDriverConfig
	reply := &models.TaskValidateResponse{}
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return reply, err
	}
	uri := driverConfig.ConnectionConfig.GetDBUri()
	db, err := sql.CreateDB(uri)
	if err != nil {
		return reply, err
	}

	query := `select @@global.version`
	var mysqlVersion string
	if err := db.QueryRow(query).Scan(&mysqlVersion); err != nil {
		reply.Connection.Success = false
		reply.Connection.Error = err.Error()
	} else {
		reply.Connection.Success = true
	}

	query = `select @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		reply.LogSlaveUpdates.Success = false
		reply.LogSlaveUpdates.Error = err.Error()
	}
	if !logSlaveUpdates {
		reply.LogSlaveUpdates.Success = false
		reply.LogSlaveUpdates.Error = fmt.Sprintf("%s:%d must have log_slave_updates enabled", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
	} else {
		reply.LogSlaveUpdates.Success = true
	}

	query = `SELECT @@GTID_MODE`
	var gtidMode string
	if err := db.QueryRow(query).Scan(&gtidMode); err != nil {
		reply.GtidMode.Success = false
		reply.GtidMode.Error = err.Error()
	}
	if gtidMode != "ON" {
		reply.GtidMode.Success = false
		reply.GtidMode.Error = fmt.Sprintf("Must have GTID enabled: %+v", gtidMode)
	} else {
		reply.GtidMode.Success = true
	}

	query = `SELECT @@SERVER_ID`
	var serverID string
	if err := db.QueryRow(query).Scan(&serverID); err != nil {
		reply.ServerID.Success = false
		reply.ServerID.Error = err.Error()
	}

	if serverID == "0" {
		reply.ServerID.Success = false
		reply.ServerID.Error = fmt.Sprintf("Master - server_id was not set")
	} else {
		reply.ServerID.Success = true
	}

	query = `select @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := db.QueryRow(query).Scan(&hasBinaryLogs, &driverConfig.BinlogFormat); err != nil {
		reply.Binlog.Success = false
		reply.Binlog.Error = err.Error()
	}
	if !hasBinaryLogs {
		reply.Binlog.Success = false
		// TODO: make sure connectConfig is source
		reply.Binlog.Error = fmt.Sprintf("%s:%d must have binary logs enabled", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
	} else if "ROW" != driverConfig.BinlogFormat {
		reply.Binlog.Success = false
		reply.Binlog.Error = fmt.Sprintf("%s:%d must be using ROW binlog format.", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
	} else {
		reply.Binlog.Success = true
	}

	query = `show grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	err = sql.QueryRowsMap(db, query, func(rowMap sql.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) {
				foundSuper = true
			}
			if strings.Contains(grant, `REPLICATION CLIENT`) {
				foundReplicationClient = true
			}
			if strings.Contains(grant, `REPLICATION SLAVE`) {
				foundReplicationSlave = true
			}
			if base.StringContainsAll(grant, `SELECT`) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		reply.Privileges.Success = false
		reply.Privileges.Error = err.Error()
	}
	driverConfig.HasSuperPrivilege = foundSuper

	if foundAll {
		reply.Privileges.Success = true
	} else if foundSuper && foundReplicationSlave && foundDBAll {
		reply.Privileges.Success = true
	} else if foundReplicationClient && foundReplicationSlave && foundDBAll {
		reply.Privileges.Success = true
	} else {
		reply.Privileges.Success = false
		reply.Privileges.Error = fmt.Sprintf("User has insufficient privileges for extractor. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on *.*")
	}
	return reply, nil
}
