/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"reflect"
	"sync"
	"testing"
	"github.com/actiontech/dtle/internal/client/driver"
	"github.com/actiontech/dtle/internal/config"
	log "github.com/actiontech/dtle/internal/logger"
	"github.com/actiontech/dtle/internal/models"
)

func TestNewWorker(t *testing.T) {
	type args struct {
		logger      *log.Logger
		config      *config.ClientConfig
		updater     TaskStateUpdater
		alloc       *models.Allocation
		task        *models.Task
		workUpdates chan *models.TaskUpdate
	}
	tests := []struct {
		name string
		args args
		want *Worker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewWorker(tt.args.logger, tt.args.config, tt.args.updater, tt.args.alloc, tt.args.task, tt.args.workUpdates); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_MarkReceived(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.MarkReceived()
		})
	}
}

func TestWorker_WaitCh(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   <-chan struct{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if got := r.WaitCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.WaitCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_stateFilePath(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if got := r.stateFilePath(); got != tt.want {
				t.Errorf("Worker.stateFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_SaveState(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if err := r.SaveState(); (err != nil) != tt.wantErr {
				t.Errorf("Worker.SaveState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_DestroyState(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if err := r.DestroyState(); (err != nil) != tt.wantErr {
				t.Errorf("Worker.DestroyState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_setState(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		state string
		event *models.TaskEvent
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.setState(tt.args.state, tt.args.event)
		})
	}
}

func TestWorker_createDriver(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Driver
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			got, err := r.createDriver()
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.createDriver() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.createDriver() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_Run(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.Run()
		})
	}
}

func TestWorker_prestart(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		resultCh chan bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.prestart(tt.args.resultCh)
		})
	}
}

func TestWorker_run(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.run()
		})
	}
}

func TestWorker_shouldRestart(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if got := r.shouldRestart(); got != tt.want {
				t.Errorf("Worker.shouldRestart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_killTask(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		killingEvent *models.TaskEvent
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.killTask(tt.args.killingEvent)
		})
	}
}

func TestWorker_startTask(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if err := r.startTask(); (err != nil) != tt.wantErr {
				t.Errorf("Worker.startTask() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_collectResourceUsageStats(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		stopCollection <-chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.collectResourceUsageStats(tt.args.stopCollection)
		})
	}
}

func TestWorker_LatestTaskStats(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *models.TaskStatistics
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if got := r.LatestTaskStats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.LatestTaskStats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_handleDestroy(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	tests := []struct {
		name          string
		fields        fields
		wantDestroyed bool
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			gotDestroyed, err := r.handleDestroy()
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.handleDestroy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDestroyed != tt.wantDestroyed {
				t.Errorf("Worker.handleDestroy() = %v, want %v", gotDestroyed, tt.wantDestroyed)
			}
		})
	}
}

func TestWorker_Restart(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		source string
		reason string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.Restart(tt.args.source, tt.args.reason)
		})
	}
}

func TestWorker_Kill(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		source string
		reason string
		fail   bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.Kill(tt.args.source, tt.args.reason, tt.args.fail)
		})
	}
}

func TestWorker_UnblockStart(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		source string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.UnblockStart(tt.args.source)
		})
	}
}

func TestWorker_waitErrorToEvent(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		res *models.WaitResult
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *models.TaskEvent
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			if got := r.waitErrorToEvent(tt.args.res); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.waitErrorToEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_Destroy(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		event *models.TaskEvent
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.Destroy(tt.args.event)
		})
	}
}

func TestWorker_emitStats(t *testing.T) {
	type fields struct {
		config          *config.ClientConfig
		updater         TaskStateUpdater
		logger          *log.Logger
		alloc           *models.Allocation
		restartTracker  *RestartTracker
		running         bool
		runningLock     sync.Mutex
		taskStats       *models.TaskStatistics
		taskStatsLock   sync.RWMutex
		task            *models.Task
		handle          driver.DriverHandle
		handleLock      sync.Mutex
		payloadRendered bool
		startCh         chan struct{}
		unblockCh       chan struct{}
		unblocked       bool
		unblockLock     sync.Mutex
		restartCh       chan *models.TaskEvent
		destroy         bool
		destroyCh       chan struct{}
		destroyLock     sync.Mutex
		destroyEvent    *models.TaskEvent
		workUpdates     chan *models.TaskUpdate
		waitCh          chan struct{}
		persistLock     sync.Mutex
	}
	type args struct {
		ru *models.TaskStatistics
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Worker{
				config:          tt.fields.config,
				updater:         tt.fields.updater,
				logger:          tt.fields.logger,
				alloc:           tt.fields.alloc,
				restartTracker:  tt.fields.restartTracker,
				running:         tt.fields.running,
				runningLock:     tt.fields.runningLock,
				taskStats:       tt.fields.taskStats,
				taskStatsLock:   tt.fields.taskStatsLock,
				task:            tt.fields.task,
				handle:          tt.fields.handle,
				handleLock:      tt.fields.handleLock,
				payloadRendered: tt.fields.payloadRendered,
				startCh:         tt.fields.startCh,
				unblockCh:       tt.fields.unblockCh,
				unblocked:       tt.fields.unblocked,
				unblockLock:     tt.fields.unblockLock,
				restartCh:       tt.fields.restartCh,
				destroy:         tt.fields.destroy,
				destroyCh:       tt.fields.destroyCh,
				destroyLock:     tt.fields.destroyLock,
				destroyEvent:    tt.fields.destroyEvent,
				workUpdates:     tt.fields.workUpdates,
				waitCh:          tt.fields.waitCh,
				persistLock:     tt.fields.persistLock,
			}
			r.emitStats(tt.args.ru)
		})
	}
}
