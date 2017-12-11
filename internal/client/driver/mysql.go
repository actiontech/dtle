package driver

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"strings"

	"udup/internal/client/driver/mysql"
	ubase "udup/internal/client/driver/mysql/base"
	usql "udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	"udup/internal/models"
)

type MySQLDriver struct {
	DriverContext
}

// NewMySQLDriver is used to create a new mysql driver
func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

// Validate is used to validate the driver configuration
func (d *MySQLDriver) Validate(task *models.Task) error {
	var driverConfig config.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}
	uri := driverConfig.ConnectionConfig.GetDBUri()
	db, err := usql.CreateDB(uri)
	if err != nil {
		return err
	}

	validateConnection := `select @@global.version`
	if _, err := db.Query(validateConnection); err != nil {
		return err
	}

	if d.taskName == models.TaskTypeSrc {
		query := `select @@global.log_slave_updates`
		var logSlaveUpdates bool
		if err := db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
			return err
		}

		if logSlaveUpdates {
			return fmt.Errorf("log_slave_updates validated on %s:%d", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
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
			return err
		}
		driverConfig.HasSuperPrivilege = foundSuper

		if foundAll {
			return nil
		}
		if foundSuper && foundReplicationSlave && foundDBAll {
			return nil
		}
		if foundReplicationClient && foundReplicationSlave && foundDBAll {
			return nil
		}
		d.logger.Debugf("Privileges: super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on *.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, foundDBAll)
		return fmt.Errorf("User has insufficient privileges for extractor. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on *.*")

		query = `SELECT @@GTID_MODE`
		var gtidMode string
		if err := db.QueryRow(query).Scan(&gtidMode); err != nil {
			return err
		}
		if gtidMode != "ON" {
			return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
		}

		query = `select @@global.log_bin, @@global.binlog_format`
		var hasBinaryLogs bool
		if err := db.QueryRow(query).Scan(&hasBinaryLogs, &driverConfig.BinlogFormat); err != nil {
			return err
		}
		if !hasBinaryLogs {
			return fmt.Errorf("%s:%d must have binary logs enabled", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
		}
		if driverConfig.RequiresBinlogFormatChange() {
			return fmt.Errorf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s:%d doesn't have replicas", driverConfig.ConnectionConfig.Host, driverConfig.ConnectionConfig.Port)
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
				if strings.Contains(grant, "GRANT ALL PRIVILEGES ON `actiontech_udup`.`gtid_executed`") {
					foundDBAll = true
				}
				if ubase.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON`) {
					foundDBAll = true
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		driverConfig.HasSuperPrivilege = foundSuper

		if foundAll {
			return nil
		}
		if foundSuper {
			return nil
		}
		if foundDBAll {
			return nil
		}
		return fmt.Errorf("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
	}
	return nil
}

func (m *MySQLDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig config.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch task.Type {
	case models.TaskTypeSrc:
		{
			// Create the extractor
			e := mysql.NewExtractor(ctx.Subject, ctx.Tp, ctx.MaxPayload, &driverConfig, m.logger)
			go e.Run()
			return e, nil
		}
	case models.TaskTypeDest:
		{
			a := mysql.NewApplier(ctx.Subject, ctx.Tp, &driverConfig, m.logger)
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
