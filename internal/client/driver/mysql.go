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

	switch ctx.Tp {
	case models.JobTypeSync:
		{
			switch task.Type {
			case models.TaskTypeSrc:
				{
					// Create the extractor
					e := mysql.NewExtractor(ctx.Subject, &driverConfig, m.logger)
					go e.Run()
					return e, nil
				}
			case models.TaskTypeDest:
				{
					a := mysql.NewApplier(ctx.Subject, &driverConfig, m.logger)
					go a.Run()
					return a, nil
				}
			default:
				{
					return nil, fmt.Errorf("unknown processor type : %+v", task.Type)
				}
			}
		}
	case models.JobTypeMig:
		{
			switch task.Type {
			case models.TaskTypeSrc:
				{
					// Create the extractor
					e := mysql.NewExtractor(ctx.Subject, &driverConfig, m.logger)
					go e.Run()
					return e, nil
				}
			case models.TaskTypeDest:
				{
					a := mysql.NewApplier(ctx.Subject, &driverConfig, m.logger)
					go a.Run()
					return a, nil
				}
			default:
				{
					return nil, fmt.Errorf("unknown processor type : %+v", task.Type)
				}
			}
		}
	case models.JobTypeSub:
		{
			switch task.Type {
			case models.TaskTypeSrc:
				{
					// Create the extractor
					e := mysql.NewExtractor(ctx.Subject, &driverConfig, m.logger)
					go e.Run()
					return e, nil
				}
			case models.TaskTypeDest:
				{
					a := mysql.NewApplier(ctx.Subject, &driverConfig, m.logger)
					go a.Run()
					return a, nil
				}
			default:
				{
					return nil, fmt.Errorf("unknown processor type : %+v", task.Type)
				}
			}
		}
	default:
		{
			return nil, fmt.Errorf("unknown job type : %+v", ctx.Tp)
		}
	}

	return nil, nil
}
