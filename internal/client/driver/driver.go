/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package driver

import (
	"errors"
	"fmt"

	"github.com/actiontech/dtle/internal/client/driver/common"
	"github.com/sirupsen/logrus"

	uconf "github.com/actiontech/dtle/internal/config"
	"github.com/actiontech/dtle/internal/models"
)

var (
	// BuiltinDrivers contains the built in registered drivers
	// which are available for allocation handling
	BuiltinDrivers = map[string]Factory{
		models.TaskDriverMySQL: NewMySQLDriver,
		models.TaskDriverKafka: NewKafkaDriver,
		//"models.TaskDriverOracle:     NewOracleDriver,
	}

	// DriverStatsNotImplemented is the error to be returned if a driver doesn't
	// implement stats.
	DriverStatsNotImplemented = errors.New("stats not implemented for driver")
)

// NewDriver is used to instantiate and return a new driver
// given the name and a logger
func NewDriver(name string, ctx *DriverContext) (Driver, error) {
	// Lookup the factory function
	factory, ok := BuiltinDrivers[name]
	if !ok {
		return nil, fmt.Errorf("unknown driver '%s'", name)
	}

	// Instantiate the driver
	f := factory(ctx)
	return f, nil
}

// Factory is used to instantiate a new Driver
type Factory func(*DriverContext) Driver

// Driver is used for execution of tasks. This allows Udup
// to support many pluggable implementations of task drivers.
type Driver interface {
	// Start is used to being task execution
	Start(ctx *common.ExecContext, task *models.Task) (DriverHandle, error)

	// Drivers must validate their configuration
	Validate(task *models.Task) (*models.TaskValidateResponse, error)
}

// DriverContext is a means to inject dependencies such as loggers, configs, and
// node attributes into a Driver without having to change the Driver interface
// each time we do it. Used in conjection with Factory, above.
type DriverContext struct {
	taskName string
	allocID  string
	config   *uconf.ClientConfig
	logger   *logrus.Logger
	node     *models.Node
}

// NewEmptyDriverContext returns a DriverContext with all fields set to their
// zero value.
func NewEmptyDriverContext() *DriverContext {
	return &DriverContext{}
}

// NewDriverContext initializes a new DriverContext with the specified fields.
// This enables other packages to create DriverContexts but keeps the fields
// private to the driver. If we want to change this later we can gorename all of
// the fields in DriverContext.
func NewDriverContext(taskName, allocID string, config *uconf.ClientConfig, node *models.Node,
	logger *logrus.Logger) *DriverContext {
	return &DriverContext{
		taskName: taskName,
		allocID:  allocID,
		config:   config,
		node:     node,
		logger:   logger,
	}
}

// DriverHandle is an opaque handle into a driver used for task
// manipulation
type DriverHandle interface {
	// Returns an opaque handle that can be used to re-open the handle
	ID() string

	// WaitChan is used to return a channel used wait for task completion
	WaitCh() chan *models.WaitResult

	// Shutdown is used to stop the task
	Shutdown() error

	// Stats returns aggregated stats of the driver
	Stats() (*models.TaskStatistics, error)
}
