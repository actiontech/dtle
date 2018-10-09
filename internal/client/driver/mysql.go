/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package driver

import (
	"fmt"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/actiontech/dtle/internal/client/driver/mysql"
	ubase "github.com/actiontech/dtle/internal/client/driver/mysql/base"
	usql "github.com/actiontech/dtle/internal/client/driver/mysql/sql"
	"github.com/actiontech/dtle/internal/config"
	"github.com/actiontech/dtle/internal/models"

	"github.com/actiontech/dtle/internal/g"
)

type MySQLDriver struct {
	DriverContext
}

// NewMySQLDriver is used to create a new mysql driver
func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

// Validate is used to validate the driver configuration
func (m *MySQLDriver) Validate(task *models.Task) (*models.TaskValidateResponse, error) {
	var driverConfig config.MySQLDriverConfig
	reply := &models.TaskValidateResponse{}
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return reply, err
	}
	uri := driverConfig.ConnectionConfig.GetDBUri()
	db, err := usql.CreateDB(uri)
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

	if task.Type == models.TaskTypeSrc {
		var query string

		// Get max allowed packet size
		/*query = `select @@global.max_allowed_packet;`
		var maxAllowedPacket int
		if err := db.QueryRow(query).Scan(&maxAllowedPacket); err != nil {
			reply.MaxAllowedPacket.Success = false
			reply.MaxAllowedPacket.Error = err.Error()
		}
		if maxAllowedPacket < 2048 {
			reply.MaxAllowedPacket.Success = false
			reply.MaxAllowedPacket.Error = fmt.Sprintf("%s:%d must set global max_allowed_packet >= 2048", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
		} else {
			reply.MaxAllowedPacket.Success = true
		}*/

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
			rows, err := db.Query("show master status")
			if err != nil {
				reply.GtidMode.Success = false
				reply.GtidMode.Error = err.Error()
			} else {
				_, err = ubase.ParseBinlogCoordinatesFromRows(rows)
				if err != nil {
					reply.GtidMode.Success = false
					reply.GtidMode.Error = err.Error()
				} else {
					reply.GtidMode.Success = true
				}
			}
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
			reply.Binlog.Error = fmt.Sprintf("%s:%d must have binary logs enabled", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
		} else if driverConfig.RequiresBinlogFormatChange() {
			reply.Binlog.Success = false
			reply.Binlog.Error = fmt.Sprintf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s:%d doesn't have replicas", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
		} else {
			reply.Binlog.Success = true
		}

		query = `show grants for current_user()`
		foundAll := false
		foundSuper := false
		foundReplicationClient := false
		foundReplicationSlave := false
		foundDBAll := false

		err = usql.QueryRowsMap(db, query, func(rowMap usql.RowMap) error {
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
				if ubase.StringContainsAll(grant, `SELECT`) {
					foundDBAll = true
				}
				if ubase.StringContainsAll(grant, `SELECT`) {
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
	} else {
		query := `show grants for current_user()`
		foundAll := false
		foundSuper := false
		foundDBAll := false

		err := usql.QueryRowsMap(db, query, func(rowMap usql.RowMap) error {
			for _, grantData := range rowMap {
				grant := grantData.String
				if strings.Contains(grant, `GRANT ALL PRIVILEGES ON`) {
					foundAll = true
				}
				if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
					foundSuper = true
				}
				if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%v`.`gtid_executed`", g.DtleSchemaName)) {
					foundDBAll = true
				}
				if ubase.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON`) {
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
		} else if foundSuper {
			reply.Privileges.Success = true
		} else if foundDBAll {
			reply.Privileges.Success = true
		} else {
			reply.Privileges.Success = false
			reply.Privileges.Error = fmt.Sprintf("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
		}
	}
	if task.Config["ExpandSyntaxSupport"] == true {
		if _, err := db.Query("use mysql"); err != nil {
			reply.Privileges.Success = false
			reply.Privileges.Error = err.Error()
		}
	}
	return reply, nil
}

func (m *MySQLDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig config.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch task.Type {
	case models.TaskTypeSrc:
		{
			m.logger.Debugf("NewExtractor ReplicateDoDb: %v", driverConfig.ReplicateDoDb)
			// Create the extractor
			e, err := mysql.NewExtractor(ctx.Subject, ctx.Tp, ctx.MaxPayload, &driverConfig, m.logger)
			if err != nil {
				return nil, err
			}
			go e.Run()
			return e, nil
		}
	case models.TaskTypeDest:
		{
			m.logger.Debugf("NewApplier ReplicateDoDb: %v", driverConfig.ReplicateDoDb)
			a, err := mysql.NewApplier(ctx.Subject, ctx.Tp, &driverConfig, m.logger)
			if err != nil {
				return nil, err
			}
			go a.Run()
			return a, nil
		}
	default:
		{
			return nil, fmt.Errorf("unknown processor type : %+v", task.Type)
		}
	}

	return nil, nil
}
