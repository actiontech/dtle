package driver

import (
	"fmt"
	"time"

	"udup/client/config"
	mysqldriver "udup/client/driver/mysql"
	"udup/client/driver/mysql/base"
	"udup/server/structs"

	"github.com/mitchellh/mapstructure"
	"github.com/nats-io/go-nats"
)

const (
	// The key populated in Node Attributes to indicate the presence of the Exec
	// driver
	mysqlDriverAttr = "driver.mysql"
)

// MySQLDriver fork/execs tasks using as many of the underlying OS's isolation
// features.
type MySQLDriver struct {
	DriverContext
}

// NewExecDriver is used to create a new exec driver
func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Fingerprint(cfg *config.Config, node *structs.Node) (bool, error) {
	// Get the current status so that we can log any debug messages only if the
	// state changes
	_, currentlyEnabled := node.Attributes[mysqlDriverAttr]

	if !currentlyEnabled {
		d.logger.Printf("[DEBUG] driver.mysql: mysql driver is enabled")
	}
	node.Attributes[mysqlDriverAttr] = "1"
	return true, nil
}

func (d *MySQLDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

func (d *MySQLDriver) Sync(ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	switch task.Name {
	case structs.TaskTypeExtract:
		{

			task.Config["NatsServerAddr"] = d.DriverContext.node.NatsAddr
			nc, err := nats.Connect(fmt.Sprintf("nats://%s", driverConfig.NatsServerAddr))
			if err != nil {
				return err
			}

			c, _ := nats.NewEncodedConn(nc, nats.GOB_ENCODER)

			if _, err := mysqldriver.InitiateExtracter(d.logger, &driverConfig, c, structs.JobTypeSync); err != nil {
				return err
			}
			return nil
		}
	case structs.TaskTypeReplay:
		{
			nc, err := nats.Connect(fmt.Sprintf("nats://%s", driverConfig.NatsServerAddr))
			if err != nil {
				return err
			}

			c, _ := nats.NewEncodedConn(nc, nats.GOB_ENCODER)

			if _, err := mysqldriver.InitiateReplayer(d.logger, &driverConfig, c); err != nil {
				return err
			}

			return nil
		}
	default:
		{
			return fmt.Errorf("Unknown task : %+v", task.Name)
		}
	}

	return nil
}

func (d *MySQLDriver) Migrate(ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	return nil
}

func (d *MySQLDriver) Sub(ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	return nil
}
