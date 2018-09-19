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
	"github.com/actiontech/udup/internal/config"
	log "github.com/actiontech/udup/internal/logger"
	"github.com/actiontech/udup/internal/models"
)

func TestNewAllocator(t *testing.T) {
	type args struct {
		logger      *log.Logger
		config      *config.ClientConfig
		updater     AllocStateUpdater
		alloc       *models.Allocation
		workUpdates chan *models.TaskUpdate
	}
	tests := []struct {
		name string
		args args
		want *Allocator
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewAllocator(tt.args.logger, tt.args.config, tt.args.updater, tt.args.alloc, tt.args.workUpdates); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAllocator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_stateFilePath(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.stateFilePath(); got != tt.want {
				t.Errorf("Allocator.stateFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_SaveState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if err := r.SaveState(); (err != nil) != tt.wantErr {
				t.Errorf("Allocator.SaveState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAllocator_saveAllocatorState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if err := r.saveAllocatorState(); (err != nil) != tt.wantErr {
				t.Errorf("Allocator.saveAllocatorState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAllocator_saveWorkerState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		tr *Worker
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if err := r.saveWorkerState(tt.args.tr); (err != nil) != tt.wantErr {
				t.Errorf("Allocator.saveWorkerState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAllocator_DestroyState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if err := r.DestroyState(); (err != nil) != tt.wantErr {
				t.Errorf("Allocator.DestroyState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_copyTaskStates(t *testing.T) {
	type args struct {
		states map[string]*models.TaskState
	}
	tests := []struct {
		name string
		args args
		want map[string]*models.TaskState
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyTaskStates(tt.args.states); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copyTaskStates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_Alloc(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *models.Allocation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.Alloc(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Allocator.Alloc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_dirtySyncState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.dirtySyncState()
		})
	}
}

func TestAllocator_syncStatus(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if err := r.syncStatus(); (err != nil) != tt.wantErr {
				t.Errorf("Allocator.syncStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAllocator_setStatus(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		status string
		desc   string
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.setStatus(tt.args.status, tt.args.desc)
		})
	}
}

func TestAllocator_setTaskState(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		taskName string
		state    string
		event    *models.TaskEvent
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.setTaskState(tt.args.taskName, tt.args.state, tt.args.event)
		})
	}
}

func TestAllocator_appendTaskEvent(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		state *models.TaskState
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.appendTaskEvent(tt.args.state, tt.args.event)
		})
	}
}

func TestAllocator_Run(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.Run()
		})
	}
}

func TestAllocator_destroyWorkers(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		destroyEvent *models.TaskEvent
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.destroyWorkers(tt.args.destroyEvent)
		})
	}
}

func TestAllocator_handleDestroy(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.handleDestroy()
		})
	}
}

func TestAllocator_Update(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		update *models.Allocation
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.Update(tt.args.update)
		})
	}
}

func TestAllocator_StatsReporter(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   AllocStatsReporter
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.StatsReporter(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Allocator.StatsReporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_getWorkers(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   []*Worker
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.getWorkers(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Allocator.getWorkers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_LatestAllocStats(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		taskFilter string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.AllocStatistics
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			got, err := r.LatestAllocStats(tt.args.taskFilter)
			if (err != nil) != tt.wantErr {
				t.Errorf("Allocator.LatestAllocStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Allocator.LatestAllocStats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_shouldUpdate(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	type args struct {
		serverIndex uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.shouldUpdate(tt.args.serverIndex); got != tt.want {
				t.Errorf("Allocator.shouldUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocator_Destroy(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			r.Destroy()
		})
	}
}

func TestAllocator_WaitCh(t *testing.T) {
	type fields struct {
		config                 *config.ClientConfig
		updater                AllocStateUpdater
		logger                 *log.Logger
		alloc                  *models.Allocation
		allocClientStatus      string
		allocClientDescription string
		allocLock              sync.Mutex
		dirtyCh                chan struct{}
		tasks                  map[string]*Worker
		taskStates             map[string]*models.TaskState
		restored               map[string]struct{}
		taskLock               sync.RWMutex
		taskStatusLock         sync.RWMutex
		updateCh               chan *models.Allocation
		workUpdates            chan *models.TaskUpdate
		destroy                bool
		destroyCh              chan struct{}
		destroyLock            sync.Mutex
		waitCh                 chan struct{}
		persistLock            sync.Mutex
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
			r := &Allocator{
				config:                 tt.fields.config,
				updater:                tt.fields.updater,
				logger:                 tt.fields.logger,
				alloc:                  tt.fields.alloc,
				allocClientStatus:      tt.fields.allocClientStatus,
				allocClientDescription: tt.fields.allocClientDescription,
				allocLock:              tt.fields.allocLock,
				dirtyCh:                tt.fields.dirtyCh,
				tasks:                  tt.fields.tasks,
				taskStates:             tt.fields.taskStates,
				restored:               tt.fields.restored,
				taskLock:               tt.fields.taskLock,
				taskStatusLock:         tt.fields.taskStatusLock,
				updateCh:               tt.fields.updateCh,
				workUpdates:            tt.fields.workUpdates,
				destroy:                tt.fields.destroy,
				destroyCh:              tt.fields.destroyCh,
				destroyLock:            tt.fields.destroyLock,
				waitCh:                 tt.fields.waitCh,
				persistLock:            tt.fields.persistLock,
			}
			if got := r.WaitCh(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Allocator.WaitCh() = %v, want %v", got, tt.want)
			}
		})
	}
}
