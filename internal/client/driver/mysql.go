package driver

import (
	"fmt"

	"github.com/mitchellh/mapstructure"

	"udup/internal/client/driver/mysql"
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

func (m *MySQLDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig config.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch task.Type {
	case models.TaskTypeSrc:
		{
			// Create the extractor
			e := mysql.NewExtractor(ctx.Subject, ctx.Tp, &driverConfig, m.logger)
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
