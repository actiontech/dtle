package driver

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"

	umysql "udup/internal/client/driver/mysql"
	uconf "udup/internal/config"
	"udup/internal/models"
)

type MySQLDriver struct {
	DriverContext
}

// NewMySQLDriver is used to create a new mysql driver
func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	id := &uconf.DriverCtx{}
	if err := json.Unmarshal([]byte(handleID), id); err != nil {
		return nil, fmt.Errorf("Failed to parse handle '%s': %v", handleID, err)
	}
	switch ctx.Tp {
	case models.TaskTypeSrc:
		{
			// Return a driver handle
			e := umysql.NewExtractor(ctx.Subject, id.DriverConfig, d.logger)
			if err := e.Run(); err != nil {
				return nil, err
			}
			return e, nil
		}
	case models.TaskTypeDest:
		{
			// Return a driver handle
			a := umysql.NewApplier(ctx.Subject, id.DriverConfig, d.logger)
			if err := a.Run(); err != nil {
				return nil, err
			}
			return a, nil
		}
	default:
		{
			return nil, fmt.Errorf("unknown processor type : %+v", ctx.Tp)
		}
	}

	return nil, nil
}

func (d *MySQLDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig uconf.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch ctx.Tp {
	case models.TaskTypeSrc:
		{
			// Create the extractor
			e := umysql.NewExtractor(ctx.Subject, &driverConfig, d.logger)
			if err := e.Run(); err != nil {
				return nil, err
			}
			return e, nil
		}
	case models.TaskTypeDest:
		{
			a := umysql.NewApplier(ctx.Subject, &driverConfig, d.logger)
			if err := a.Run(); err != nil {
				return nil, err
			}
			return a, nil
		}
	default:
		{
			return nil, fmt.Errorf("unknown processor type : %+v", ctx.Tp)
		}
	}
	return nil, nil
}
