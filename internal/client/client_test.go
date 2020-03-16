/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
	"github.com/actiontech/dts/internal/config"
	ulog "github.com/actiontech/dts/internal/logger"
	"github.com/actiontech/dts/internal/models"
	"github.com/actiontech/dts/internal/server"

	stand "github.com/nats-io/nats-streaming-server/server"
)

func Test_newMigrateAllocCtrl(t *testing.T) {
	type args struct {
		alloc *models.Allocation
	}
	tests := []struct {
		name string
		args args
		want *migrateAllocCtrl
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newMigrateAllocCtrl(tt.args.alloc); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newMigrateAllocCtrl() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_migrateAllocCtrl_closeCh(t *testing.T) {
	type fields struct {
		alloc  *models.Allocation
		ch     chan struct{}
		closed bool
		chLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &migrateAllocCtrl{
				alloc:  tt.fields.alloc,
				ch:     tt.fields.ch,
				closed: tt.fields.closed,
				chLock: tt.fields.chLock,
			}
			m.closeCh()
		})
	}
}

func TestNewClient(t *testing.T) {
	type args struct {
		cfg    *config.ClientConfig
		logger *ulog.Logger
	}
	tests := []struct {
		name    string
		args    args
		want    *Client
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewClient(tt.args.cfg, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_init(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.init(); (err != nil) != tt.wantErr {
				t.Errorf("Client.init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Leave(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.Leave(); (err != nil) != tt.wantErr {
				t.Errorf("Client.Leave() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Datacenter(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.Datacenter(); got != tt.want {
				t.Errorf("Client.Datacenter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Region(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.Region(); got != tt.want {
				t.Errorf("Client.Region() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Shutdown(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Client.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_RPC(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		method string
		args   interface{}
		reply  interface{}
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.RPC(tt.args.method, tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Client.RPC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Stats(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]map[string]string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Node(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *models.Node
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.Node(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.Node() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_StatsReporter(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   ClientStatsReporter
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.StatsReporter(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.StatsReporter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetAllocStats(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		allocID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    AllocStatsReporter
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := c.GetAllocStats(tt.args.allocID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetAllocStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.GetAllocStats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetClientAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		allocID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Allocation
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := c.GetClientAlloc(tt.args.allocID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetClientAlloc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.GetClientAlloc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetServers(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.GetServers(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.GetServers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_SetServers(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		servers []string
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.SetServers(tt.args.servers); (err != nil) != tt.wantErr {
				t.Errorf("Client.SetServers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_saveState(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.saveState(); (err != nil) != tt.wantErr {
				t.Errorf("Client.saveState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_getAllocRunners(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]*Allocator
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.getAllocRunners(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.getAllocRunners() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_nodeID(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantId  string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			gotId, err := c.nodeID()
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.nodeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotId != tt.wantId {
				t.Errorf("Client.nodeID() = %v, want %v", gotId, tt.wantId)
			}
		})
	}
}

func TestClient_setupNode(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.setupNode(); (err != nil) != tt.wantErr {
				t.Errorf("Client.setupNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_setupNatsServer(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.setupNatsServer(); (err != nil) != tt.wantErr {
				t.Errorf("Client.setupNatsServer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_setupDrivers(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.setupDrivers(); (err != nil) != tt.wantErr {
				t.Errorf("Client.setupDrivers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_retryIntv(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		base time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.retryIntv(tt.args.base); got != tt.want {
				t.Errorf("Client.retryIntv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_registerAndHeartbeat(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.registerAndHeartbeat()
		})
	}
}

func TestClient_periodicSnapshot(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.periodicSnapshot()
		})
	}
}

func TestClient_run(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.run()
		})
	}
}

func TestClient_hasNodeChanged(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		oldAttrHash uint64
		oldMetaHash uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
		want1  uint64
		want2  uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, got1, got2 := c.hasNodeChanged(tt.args.oldAttrHash, tt.args.oldMetaHash)
			if got != tt.want {
				t.Errorf("Client.hasNodeChanged() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Client.hasNodeChanged() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("Client.hasNodeChanged() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestClient_retryRegisterNode(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.retryRegisterNode()
		})
	}
}

func TestClient_registerNode(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.registerNode(); (err != nil) != tt.wantErr {
				t.Errorf("Client.registerNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_updateNodeStatus(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.updateNodeStatus(); (err != nil) != tt.wantErr {
				t.Errorf("Client.updateNodeStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_updateAllocStatus(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		alloc *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.updateAllocStatus(tt.args.alloc)
		})
	}
}

func TestClient_allocSync(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.allocSync()
		})
	}
}

func TestClient_watchAllocations(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		updates  chan *allocUpdates
		jUpdates chan *jobUpdates
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.watchAllocations(tt.args.updates, tt.args.jUpdates)
		})
	}
}

func TestClient_watchNodeUpdates(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.watchNodeUpdates()
		})
	}
}

func TestClient_runAllocs(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		update *allocUpdates
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.runAllocs(tt.args.update)
		})
	}
}

func TestClient_blockForRemoteAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		alloc *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.blockForRemoteAlloc(tt.args.alloc)
		})
	}
}

func TestClient_waitForAllocTerminal(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		allocID string
		stopCh  *migrateAllocCtrl
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Allocation
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := c.waitForAllocTerminal(tt.args.allocID, tt.args.stopCh)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.waitForAllocTerminal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.waitForAllocTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_getNode(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		nodeID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Node
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			got, err := c.getNode(tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.getNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.getNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_removeAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		alloc *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.removeAlloc(tt.args.alloc); (err != nil) != tt.wantErr {
				t.Errorf("Client.removeAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_updateAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		exist  *models.Allocation
		update *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.updateAlloc(tt.args.exist, tt.args.update); (err != nil) != tt.wantErr {
				t.Errorf("Client.updateAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_resumeAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		alloc *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.resumeAlloc(tt.args.alloc); (err != nil) != tt.wantErr {
				t.Errorf("Client.resumeAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_addAlloc(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	type args struct {
		alloc *models.Allocation
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
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if err := c.addAlloc(tt.args.alloc); (err != nil) != tt.wantErr {
				t.Errorf("Client.addAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_triggerDiscovery(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.triggerDiscovery()
		})
	}
}

func TestClient_emitClientMetrics(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			c.emitClientMetrics()
		})
	}
}

func TestClient_allAllocs(t *testing.T) {
	type fields struct {
		config              *config.ClientConfig
		start               time.Time
		configCopy          *config.ClientConfig
		configLock          sync.RWMutex
		logger              *ulog.Logger
		connPool            *server.ConnPool
		servers             *serverlist
		lastHeartbeat       time.Time
		heartbeatTTL        time.Duration
		heartbeatLock       sync.Mutex
		triggerDiscoveryCh  chan struct{}
		serversDiscoveredCh chan struct{}
		allocs              map[string]*Allocator
		allocLock           sync.RWMutex
		blockedAllocations  map[string]*models.Allocation
		blockedAllocsLock   sync.RWMutex
		migratingAllocs     map[string]*migrateAllocCtrl
		migratingAllocsLock sync.Mutex
		allocUpdates        chan *models.Allocation
		workUpdates         chan *models.TaskUpdate
		stand               *stand.StanServer
		shutdown            bool
		shutdownCh          chan struct{}
		shutdownLock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]*models.Allocation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				config:              tt.fields.config,
				start:               tt.fields.start,
				configCopy:          tt.fields.configCopy,
				configLock:          tt.fields.configLock,
				logger:              tt.fields.logger,
				connPool:            tt.fields.connPool,
				servers:             tt.fields.servers,
				lastHeartbeat:       tt.fields.lastHeartbeat,
				heartbeatTTL:        tt.fields.heartbeatTTL,
				heartbeatLock:       tt.fields.heartbeatLock,
				triggerDiscoveryCh:  tt.fields.triggerDiscoveryCh,
				serversDiscoveredCh: tt.fields.serversDiscoveredCh,
				allocs:              tt.fields.allocs,
				allocLock:           tt.fields.allocLock,
				blockedAllocations:  tt.fields.blockedAllocations,
				blockedAllocsLock:   tt.fields.blockedAllocsLock,
				migratingAllocs:     tt.fields.migratingAllocs,
				migratingAllocsLock: tt.fields.migratingAllocsLock,
				allocUpdates:        tt.fields.allocUpdates,
				workUpdates:         tt.fields.workUpdates,
				stand:               tt.fields.stand,
				shutdown:            tt.fields.shutdown,
				shutdownCh:          tt.fields.shutdownCh,
				shutdownLock:        tt.fields.shutdownLock,
			}
			if got := c.allAllocs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.allAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_resolveServer(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    net.Addr
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveServer(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveServer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newServerList(t *testing.T) {
	tests := []struct {
		name string
		want *serverlist
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newServerList(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newServerList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_serverlist_set(t *testing.T) {
	type fields struct {
		e  endpoints
		mu sync.RWMutex
	}
	type args struct {
		in endpoints
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
			s := &serverlist{
				e:  tt.fields.e,
				mu: tt.fields.mu,
			}
			s.set(tt.args.in)
		})
	}
}

func Test_serverlist_all(t *testing.T) {
	type fields struct {
		e  endpoints
		mu sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   endpoints
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &serverlist{
				e:  tt.fields.e,
				mu: tt.fields.mu,
			}
			if got := s.all(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serverlist.all() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_serverlist_failed(t *testing.T) {
	type fields struct {
		e  endpoints
		mu sync.RWMutex
	}
	type args struct {
		e *endpoint
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
			s := &serverlist{
				e:  tt.fields.e,
				mu: tt.fields.mu,
			}
			s.failed(tt.args.e)
		})
	}
}

func Test_serverlist_good(t *testing.T) {
	type fields struct {
		e  endpoints
		mu sync.RWMutex
	}
	type args struct {
		e *endpoint
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
			s := &serverlist{
				e:  tt.fields.e,
				mu: tt.fields.mu,
			}
			s.good(tt.args.e)
		})
	}
}

func Test_endpoints_Len(t *testing.T) {
	tests := []struct {
		name string
		e    endpoints
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Len(); got != tt.want {
				t.Errorf("endpoints.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_endpoints_Less(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		e    endpoints
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("endpoints.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_endpoints_Swap(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		e    endpoints
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.Swap(tt.args.i, tt.args.j)
		})
	}
}

func Test_endpoints_String(t *testing.T) {
	tests := []struct {
		name string
		e    endpoints
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.String(); got != tt.want {
				t.Errorf("endpoints.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_endpoint_equal(t *testing.T) {
	type fields struct {
		name     string
		addr     net.Addr
		priority int
	}
	type args struct {
		o *endpoint
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
			e := &endpoint{
				name:     tt.fields.name,
				addr:     tt.fields.addr,
				priority: tt.fields.priority,
			}
			if got := e.equal(tt.args.o); got != tt.want {
				t.Errorf("endpoint.equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
