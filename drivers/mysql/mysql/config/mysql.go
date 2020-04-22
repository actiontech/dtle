package mysql

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	qldatasource "github.com/araddon/qlbridge/datasource"
	qlexpr "github.com/araddon/qlbridge/expr"
	qlvm "github.com/araddon/qlbridge/vm"
)

const (
	DefaultBindPort  int = 8191
	DefaultClusterID     = "udup-cluster"

	channelBufferSize = 600
	defaultNumRetries = 5
	defaultChunkSize  = 2000
	defaultNumWorkers = 1
	defaultMsgBytes   = 20 * 1024
)

type DriverCtx struct {
	DriverConfig *MySQLDriverConfig
}

func (d *DataSource) String() string {
	return fmt.Sprintf(d.TableSchema)
}

type MySQLDriverConfig struct {
	//Ref:http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	ReplicateDoDb         []*DataSource
	ReplicateIgnoreDb     []*DataSource
	DropTableIfExists     bool
	ExpandSyntaxSupport   bool
	ReplChanBufferSize    int64
	MsgBytesLimit         int
	TrafficAgainstLimits  int
	TotalTransferredBytes int
	MaxRetries            int64
	ChunkSize             int64
	SqlFilter             []string
	RowsEstimate          int64
	DeltaEstimate         int64
	TimeZone              string
	GroupCount            int
	GroupMaxSize          int
	GroupTimeout          int

	Gtid              string
	BinlogFile        string
	BinlogPos         int64
	GtidStart         string
	AutoGtid          bool
	BinlogRelay       bool
	NatsAddr          string
	ParallelWorkers   int
	ConnectionConfig  *ConnectionConfig
	SystemVariables   map[string]string
	HasSuperPrivilege bool
	BinlogFormat      string
	BinlogRowImage    string
	SqlMode           string
	MySQLVersion      string
	MySQLServerUuid   string
	StartTime         time.Time
	RowCopyStartTime  time.Time
	RowCopyEndTime    time.Time
	TotalDeltaCopied  int64
	TotalRowsCopied   int64
	TotalRowsReplay   int64

	Stage                string
	ApproveHeterogeneous bool
	SkipCreateDbTable    bool

	CountingRowsFlag int64
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

	if result.ConnectionConfig == nil {
		result.ConnectionConfig = &ConnectionConfig{}
	}
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

// TableName is the table configuration
// slave restrict replication to a given table
type DataSource struct {
	TableSchema            string
	TableSchemaRegex       string
	TableSchemaRenameRegex string
	TableSchemaRename      string
	TableSchemaScope       string
	Tables                 []*Table
}

type Table struct {
	TableName         string
	TableRegex        string
	TableRename       string
	TableRenameRegex  string
	TableSchema       string
	TableSchemaRename string
	Counter           int64
	ColumnMapFrom     []string
	//ColumnMapTo       []string
	//ColumnMapUseRe    bool

	OriginalTableColumns *ColumnList
	UseUniqueKey         *UniqueKey
	Iteration            int64
	ColumnMap            []int

	TableType    string
	TableEngine  string
	RowsEstimate int64

	Where string // TODO load from job description
}

func BuildColumnMapIndex(from []string, ordinals ColumnsMap) (mapIndex []int) {
	mapIndex = make([]int, len(from))
	for i, colName := range from {
		idxFrom := ordinals[colName]
		mapIndex[i] = idxFrom
	}
	return mapIndex
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

func (t *TableContext) WhereTrue(values *ColumnValues) (bool, error) {
	var m = make(map[string]interface{})
	for field, idx := range t.WhereCtx.FieldsMap {
		nCols := len(values.AbstractValues)
		if idx >= nCols {
			return false, fmt.Errorf("cannot eval 'where' predicate: no enough columns (%v < %v)", nCols, idx)
		}

		//fmt.Printf("**** type of %v %T\n", field, *values.ValuesPointers[idx])
		rawValue := *(values.AbstractValues[idx])
		var value interface{}
		if rawValue == nil {
			value = rawValue
		} else {
			switch t.Table.OriginalTableColumns.ColumnList()[idx].Type {
			case TextColumnType:
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
