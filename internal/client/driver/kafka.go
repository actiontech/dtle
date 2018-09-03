/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package driver

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"udup/internal/client/driver/kafka2"
	"udup/internal/client/driver/kafka3"
	"udup/internal/models"
)

type KafkaDriver struct {
	DriverContext
}

func (kd *KafkaDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig kafka2.KafkaConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch task.Type {
	case models.TaskTypeSrc:
		return nil, fmt.Errorf("afka can only be used on 'Dest'")
	case models.TaskTypeDest:
		runner := kafka3.NewKafkaRunner(ctx.Subject, ctx.Tp, ctx.MaxPayload, &driverConfig, kd.logger)
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
