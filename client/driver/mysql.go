package driver

import (
	"fmt"
	"log"
	"time"

	"udup/client/config"
	mysqldriver "udup/client/driver/mysql"
	"udup/client/driver/mysql/base"
	"udup/server/structs"

	"github.com/mitchellh/mapstructure"
)

const (
	// The key populated in Node Attributes to indicate the presence of the Exec
	// driver
	mysqlDriverAttr = "driver.mysql"
)

const (
	Extract = "extract"
	Load    = "load"
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

func (d *MySQLDriver) Repl(logger *log.Logger, ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	switch task.Name {
	case Extract:
		{

		}
	case Load:
		{

		}
	default:
		{
			return fmt.Errorf("Unknown task : %+v", task.Name)
		}
	}

	return nil
}

func (d *MySQLDriver) Migrate(logger *log.Logger, ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	switch task.Name {
	case Extract:
		{

		}
	case Load:
		{

		}
	default:
		{
			return fmt.Errorf("Unknown task : %+v", task.Name)
		}
	}

	return nil
}

func (d *MySQLDriver) Sub(logger *log.Logger, ctx *ExecContext, task *structs.Task) error {
	var driverConfig base.MySQLContext
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return err
	}

	switch task.Name {
	case Extract:
		{

		}
	case Load:
		{

		}
	default:
		{
			return fmt.Errorf("Unknown task : %+v", task.Name)
		}
	}

	return nil
}
