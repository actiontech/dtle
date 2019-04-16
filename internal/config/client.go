/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package config

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/internal"
	umconf "github.com/actiontech/dtle/internal/config/mysql"
	"github.com/actiontech/dtle/internal/models"

	"strings"

	qldatasource "github.com/araddon/qlbridge/datasource"
	qlexpr "github.com/araddon/qlbridge/expr"
	qlvm "github.com/araddon/qlbridge/vm"
)

// This is the default port that we use for Serf communication
const (
	DefaultBindPort  int = 8191
	DefaultClusterID     = "udup-cluster"

	channelBufferSize = 600
	defaultNumRetries = 5
	defaultChunkSize  = 2000
	defaultNumWorkers = 1
	defaultMsgBytes   = 20 * 1024
)

// RPCHandler can be provided to the Client if there is a local server
// to avoid going over the network. If not provided, the Client will
// maintain a connection pool to the servers
type RPCHandler interface {
	RPC(method string, args interface{}, reply interface{}) error
}

// Config is used to parameterize and configure the behavior of the client
type ClientConfig struct {
	// StateDir is where we store our state
	StateDir string

	// AllocDir is where we store data for allocations
	AllocDir string

	// LogOutput is the destination for logs
	LogOutput io.Writer

	// Specify the log file name. The empty string means to log to stdout.
	LogFile string

	// Region is the clients region
	Region string

	// Servers is a list of known server addresses. These are as "host:port"
	Servers []string

	// RPCHandler can be provided to avoid network traffic if the
	// server is running locally.
	RPCHandler RPCHandler

	// Node provides the base node
	Node *models.Node

	// Version is the version of the Udup client
	Version string

	// Revision is the commit number of the Udup client
	Revision string

	// ConsulConfig is this Agent's Consul configuration
	ConsulConfig *ConsulConfig

	NatsAddr string

	MaxPayload int

	// StatsCollectionInterval is the interval at which the Udup client
	// collects resource usage stats
	StatsCollectionInterval time.Duration

	// PublishNodeMetrics determines whether server is going to publish node
	// level metrics to remote Metric sinks
	PublishNodeMetrics bool

	// PublishAllocationMetrics determines whether server is going to publish
	// allocation metrics to remote Metric sinks
	PublishAllocationMetrics bool

	// LogLevel is the level of the logs to putout
	LogLevel string

	// NoHostUUID disables using the host's UUID and will force generation of a
	// random UUID.
	NoHostUUID bool
}

func (c *ClientConfig) Copy() *ClientConfig {
	nc := new(ClientConfig)
	*nc = *c
	nc.Node = nc.Node.Copy()
	nc.Servers = internal.CopySliceString(nc.Servers)
	nc.ConsulConfig = c.ConsulConfig.Copy()
	return nc
}

type DriverCtx struct {
	DriverConfig *MySQLDriverConfig
}

func (d *DataSource) String() string {
	return fmt.Sprintf(d.TableSchema)
}

type MySQLDriverConfig struct {
	DataDir     string
	MaxFileSize int64
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb                       []*DataSource
	ReplicateIgnoreDb                   []*DataSource
	DropTableIfExists                   bool
	ExpandSyntaxSupport                 bool
	ReplChanBufferSize                  int64
	MsgBytesLimit                       int
	TrafficAgainstLimits                int
	TotalTransferredBytes               int
	SkipRenamedColumns                  bool
	MaxRetries                          int64
	ChunkSize                           int64
	SqlFilter                           []string
	niceRatio                           float64
	MaxLagMillisecondsThrottleThreshold int64
	maxLoad                             umconf.LoadMap
	criticalLoad                        umconf.LoadMap
	RowsEstimate                        int64
	DeltaEstimate                       int64
	TimeZone                            string
	GroupCount                          int
	GroupMaxSize                        int
	GroupTimeout                        int // millisecond

	Gtid                     string
	GtidStart                string
	AutoGtid                 bool // For internal use. Might be changed without notification.
	NatsAddr                 string
	ParallelWorkers          int
	ConnectionConfig         *umconf.ConnectionConfig
	SystemVariables          map[string]string
	HasSuperPrivilege        bool
	BinlogFormat             string
	BinlogRowImage           string
	SqlMode                  string
	MySQLVersion             string
	MySQLServerUuid          string
	StartTime                time.Time
	RowCopyStartTime         time.Time
	RowCopyEndTime           time.Time
	LockTablesStartTime      time.Time
	RenameTablesStartTime    time.Time
	RenameTablesEndTime      time.Time
	PointOfInterestTime      time.Time
	pointOfInterestTimeMutex *sync.Mutex
	TotalDeltaCopied         int64
	TotalRowsCopied          int64
	TotalRowsReplay          int64

	Stage                string
	ApproveHeterogeneous bool
	SkipCreateDbTable    bool

	throttleMutex               *sync.Mutex
	CountingRowsFlag            int64
	UserCommandedUnpostponeFlag int64

	SkipPrivilegeCheck  bool
	SkipIncrementalCopy bool
}

func (a *MySQLDriverConfig) SetDefault() *MySQLDriverConfig {
	result := *a

	if result.MaxRetries <= 0 {
		result.MaxRetries = defaultNumRetries
	}
	if result.ChunkSize <= 0 {
		result.ChunkSize = defaultChunkSize
	}
	if result.ReplChanBufferSize <= 0 {
		result.ReplChanBufferSize = channelBufferSize
	}
	if result.ParallelWorkers <= 0 {
		result.ParallelWorkers = defaultNumWorkers
	}
	if result.MsgBytesLimit <= 0 {
		result.MsgBytesLimit = defaultMsgBytes
	}
	if result.GroupCount == 0 {
		result.GroupCount = 1
	}
	if result.GroupMaxSize == 0 {
		result.GroupMaxSize = 1
	}
	if result.GroupTimeout == 0 {
		result.GroupTimeout = 100
	}

	// TODO temporarily (or permanently) disable homogeneous replication, hetero only.
	result.ApproveHeterogeneous = true

	if "" == result.ConnectionConfig.Charset {
		result.ConnectionConfig.Charset = "utf8mb4"
	}
	return &result
}

// RequiresBinlogFormatChange is `true` when the original binlog format isn't `ROW`
func (m *MySQLDriverConfig) RequiresBinlogFormatChange() bool {
	return m.BinlogFormat != "ROW"
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (m *MySQLDriverConfig) MarkRowCopyEndTime() {
	m.RowCopyEndTime = time.Now()
}

// MarkRowCopyStartTime
func (m *MySQLDriverConfig) MarkRowCopyStartTime() {
	m.RowCopyStartTime = time.Now()
}

func (m *MySQLDriverConfig) TimeSincePointOfInterest() time.Duration {
	m.pointOfInterestTimeMutex.Lock()
	defer m.pointOfInterestTimeMutex.Unlock()

	return time.Since(m.PointOfInterestTime)
}

// ElapsedRowCopyTime returns time since starting to copy chunks of rows
func (m *MySQLDriverConfig) ElapsedRowCopyTime() time.Duration {
	if m.RowCopyStartTime.IsZero() {
		// Row copy hasn't started yet
		return 0
	}

	if m.RowCopyEndTime.IsZero() {
		return time.Since(m.RowCopyStartTime)
	}
	return m.RowCopyEndTime.Sub(m.RowCopyStartTime)
}

// GetTotalRowsCopied returns the accurate number of rows being copied (affected)
// This is not exactly the same as the rows being iterated via chunks, but potentially close enough
func (m *MySQLDriverConfig) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&m.TotalRowsCopied)
}

func (m *MySQLDriverConfig) GetTotalRowsReplay() int64 {
	return atomic.LoadInt64(&m.TotalRowsReplay)
}

func (m *MySQLDriverConfig) GetTotalDeltaCopied() int64 {
	return atomic.LoadInt64(&m.TotalDeltaCopied)
}

// ElapsedTime returns time since very beginning of the process
func (m *MySQLDriverConfig) ElapsedTime() time.Duration {
	return time.Since(m.StartTime)
}

func (m *MySQLDriverConfig) GetNiceRatio() float64 {
	//m.throttleMutex.Lock()
	//defer m.throttleMutex.Unlock()

	return m.niceRatio
}

func (m *MySQLDriverConfig) GetMaxLoad() umconf.LoadMap {
	//m.throttleMutex.Lock()
	//defer m.throttleMutex.Unlock()

	return m.maxLoad.Duplicate()
}

func (m *MySQLDriverConfig) GetCriticalLoad() umconf.LoadMap {
	//m.throttleMutex.Lock()
	//defer m.throttleMutex.Unlock()

	return m.criticalLoad.Duplicate()
}

// TableName is the table configuration
// slave restrict replication to a given table
type DataSource struct {
	TableSchema       string
	TableSchemaRegex  string
	TableSchemaRename string
	TableSchemaScope  string
	Tables            []*Table
}

type Table struct {
	TableName         string
	TableRegex        string
	TableRename       string
	TableSchema       string
	TableSchemaRename string
	Counter           int64

	OriginalTableColumns *umconf.ColumnList
	UseUniqueKey         *umconf.UniqueKey
	Iteration            int64

	TableType    string
	TableEngine  string
	RowsEstimate int64

	Where string // TODO load from job description
}

type TableContext struct {
	Table          *Table
	WhereCtx       *WhereContext
	DefChangedSent bool
}

func NewTableContext(table *Table, whereCtx *WhereContext) *TableContext {
	return &TableContext{
		Table:          table,
		WhereCtx:       whereCtx,
		DefChangedSent: false,
	}
}

func NewTable(schemaName string, tableName string) *Table {
	return &Table{
		TableSchema: schemaName,
		TableName:   tableName,
		Iteration:   0,
		Where:       "true",
	}
}

func (t *TableContext) WhereTrue(values *umconf.ColumnValues) (bool, error) {
	var m = make(map[string]interface{})
	for field, idx := range t.WhereCtx.FieldsMap {
		nCols := len(values.ValuesPointers)
		if idx >= nCols {
			return false, fmt.Errorf("cannot eval 'where' predicate: no enough columns (%v < %v)", nCols, idx)
		}

		//fmt.Printf("**** type of %v %T\n", field, *values.ValuesPointers[idx])
		rawValue := *(values.ValuesPointers[idx])
		var value interface{}
		if rawValue == nil {
			value = rawValue
		} else {
			switch t.Table.OriginalTableColumns.ColumnList()[idx].Type {
			case umconf.TextColumnType:
				bs, ok := rawValue.([]byte)
				if !ok {
					return false,
						fmt.Errorf("where_predicate. expect []byte for TextColumnType, but got %T", rawValue)
				}
				value = string(bs)
			default:
				value = rawValue
			}
		}

		m[field] = value
	}
	ctx := qldatasource.NewContextSimpleNative(m)
	val, ok := qlvm.Eval(ctx, t.WhereCtx.Ast)
	if !ok {
		return false, fmt.Errorf("cannot eval 'where' predicate with the row value")
	}
	r, ok := val.Value().(bool)
	if !ok {
		return false, fmt.Errorf("'where' predicate does not eval to bool")
	}

	return r, nil
}

type WhereContext struct {
	Where     string
	Ast       qlexpr.Node
	FieldsMap map[string]int
	IsDefault bool // is 'true'
}

func NewWhereCtx(where string, table *Table) (*WhereContext, error) {
	ast, err := qlexpr.ParseExpression(where)
	if err != nil {
		return nil, err
	} else {
		fields := qlexpr.FindAllIdentityField(ast)
		fieldsMap := make(map[string]int)
		for _, field := range fields {
			escapedFieldName := strings.ToLower(field) // TODO thorough escape
			if escapedFieldName == "true" || escapedFieldName == "false" {
				// qlbridge limitation
			} else if _, ok := fieldsMap[field]; !ok {
				if _, ok := table.OriginalTableColumns.Ordinals[field]; !ok {
					return nil, fmt.Errorf("bad 'where' for table %v.%v: field %v does not exist",
						table.TableSchema, table.TableName, field)
				} else {
					fieldsMap[field] = table.OriginalTableColumns.Ordinals[field]
				}
			} else {
				// already mapped
			}
		}

		// We parse it even it is just 'true', but use the 'IsDefault' flag to optimize.
		return &WhereContext{
			Where:     where,
			Ast:       ast,
			FieldsMap: fieldsMap,
			IsDefault: strings.ToLower(where) == "true",
		}, nil
	}
}

// DefaultConfig returns the default configuration
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		NatsAddr:                "0.0.0.0:8193",
		ConsulConfig:            DefaultConsulConfig(),
		LogOutput:               os.Stderr,
		Region:                  "global",
		StatsCollectionInterval: 1 * time.Second,
		LogLevel:                "INFO",
	}
}
