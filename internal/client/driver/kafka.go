/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package driver

import (
	"fmt"
	"github.com/actiontech/dts/internal/client/driver/common"
	"github.com/mitchellh/mapstructure"
	"github.com/actiontech/dts/internal/client/driver/kafka3"
	"github.com/actiontech/dts/internal/models"
)

type KafkaDriver struct {
	DriverContext
}

func (kd *KafkaDriver) Start(ctx *common.ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig kafka3.KafkaConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch task.Type {
	case models.TaskTypeSrc:
		return nil, fmt.Errorf("afka can only be used on 'Dest'")
	case models.TaskTypeDest:
		runner := kafka3.NewKafkaRunner(ctx, &driverConfig, kd.logger)
		go runner.Run()
		return runner, nil
	default:
		return nil, fmt.Errorf("unknown processor type : %+v", task.Type)
	}
}

func (kd *KafkaDriver) Validate(task *models.Task) (*models.TaskValidateResponse, error) {
	reply := &models.TaskValidateResponse{}

	return reply, nil
}

func NewKafkaDriver(ctx *DriverContext) Driver {
	return &KafkaDriver{DriverContext: *ctx}
}
