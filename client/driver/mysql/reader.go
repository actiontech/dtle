package mysql

import (
	gosql "database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"

	"udup/client/driver/mysql/base"
	"udup/client/driver/mysql/binlog"
	msql "udup/client/driver/mysql/sql"

	"github.com/siddontang/go-mysql/client"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type MySQLReader struct {
	db                       *gosql.DB
	mysqlContext             *base.MySQLContext
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       binlog.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint binlog.BinlogCoordinates

	connLock sync.Mutex
	conn     *client.Conn
	reMap    map[string]*regexp.Regexp
	tables   map[string]*msql.Table
	logger   *log.Logger
}

func NewMySQLReader(e *Extracter) (binlogReader *MySQLReader, err error) {
	binlogReader = &MySQLReader{
		db:                      e.db,
		mysqlContext:            e.mysqlContext,
		currentCoordinates:      binlog.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
		logger:                  e.logger,
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID:        e.mysqlContext.ServerId,
		Flavor:          "mysql",
		Host:            e.mysqlContext.ConnectionConfig.Host,
		Port:            uint16(e.mysqlContext.ConnectionConfig.Port),
		User:            e.mysqlContext.ConnectionConfig.User,
		Password:        e.mysqlContext.ConnectionConfig.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}

	binlogReader.reMap = make(map[string]*regexp.Regexp)
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(&cfg)

	return binlogReader, err
}

// ConnectBinlogStreamer
func (m *MySQLReader) ConnectBinlogStreamer(coordinates binlog.BinlogCoordinates) (err error) {
	m.currentCoordinates = coordinates
	m.logger.Printf("[INFO] client: Connecting binlog streamer at %+v", m.currentCoordinates)
	// Start sync with sepcified binlog file and position
	m.binlogStreamer, err = m.binlogSyncer.StartSync(gomysql.Position{m.currentCoordinates.LogFile, uint32(m.currentCoordinates.LogPos)})
	return err
}

func (m *MySQLReader) GetCurrentBinlogCoordinates() *binlog.BinlogCoordinates {
	m.currentCoordinatesMutex.Lock()
	defer m.currentCoordinatesMutex.Unlock()
	returnCoordinates := m.currentCoordinates
	return &returnCoordinates
}

// StreamEvents
func (m *MySQLReader) StreamEvents(eventsChannel chan<- *EventsStream) error {
	for {
		e, err := m.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		pos := gomysql.Position{m.currentCoordinates.LogFile, uint32(m.currentCoordinates.LogPos)}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			func() {
				m.currentCoordinatesMutex.Lock()
				defer m.currentCoordinatesMutex.Unlock()
				m.currentCoordinates.LogFile = string(ev.NextLogName)
				m.currentCoordinates.LogPos = int64(ev.Position)
			}()
			pos.Name = string(ev.NextLogName)
			pos.Pos = uint32(ev.Position)
			m.logger.Printf("[INFO] client: rotate to next log name: %s", ev.NextLogName)

		case *replication.RowsEvent:
			table := &msql.Table{}
			if m.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table)) {
				continue
			}
			table, err = m.getTable(string(ev.Table.Schema), string(ev.Table.Table))
			if err != nil {
				return err
			}

			var (
				sqls []string
				keys []string
				args [][]interface{}
			)
			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				sqls, keys, args, err = msql.BuildDMLInsertQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := NewEventsStream(Insert, sqls[i], args[i], keys[i], true, pos)
					eventsChannel <- se
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, keys, args, err = msql.BuildDMLUpdateQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := NewEventsStream(Update, sqls[i], args[i], keys[i], true, pos)
					eventsChannel <- se
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, keys, args, err = msql.BuildDMLDeleteQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := NewEventsStream(Del, sqls[i], args[i], keys[i], true, pos)
					eventsChannel <- se
				}
			}
		case *replication.QueryEvent:
			ok := false
			sql := string(ev.Query)
			if m.skipQueryEvent(sql, string(ev.Schema)) {
				//m.logger.Printf("[INFO] client: skip query-sql %s ,schema:%s", sql, string(ev.Schema))
				continue
			}

			//m.logger.Printf("[INFO] client: query :%s", sql)

			sqls, ok, err := msql.ResolveDDLSQL(sql)
			if err != nil {
				return fmt.Errorf("[ERR] client: parse query event failed: %v", err)
			}
			if !ok {
				continue
			}

			lastPos := pos
			pos.Pos = e.Header.LogPos
			for _, sql := range sqls {
				if m.skipQueryDDL(sql, string(ev.Schema)) {
					//m.logger.Printf("[INFO] client: skip query-ddl-sql %s ,schema:%s", sql, ev.Schema)
					continue
				}

				sql, err = msql.BuildDDLSQL(sql, string(ev.Schema))
				if err != nil {
					m.logger.Printf("[INFO] client: BuildDDLSQL err %v", err)
					return err
				}

				m.logger.Printf("[INFO] client: ddl start %s pos %v ,next pos %v ,schema %s", sql, lastPos, pos, string(ev.Schema))

				se := NewEventsStream(Ddl, sql, nil, "", false, pos)
				eventsChannel <- se

				//m.logger.Printf("[INFO] client: ddl end %s pos %v ,next pos %v", sql, lastPos, pos)

				m.clearTables()
			}
		case *replication.XIDEvent:
			pos.Pos = e.Header.LogPos
			se := NewEventsStream(Xid, "", nil, "", false, pos)
			eventsChannel <- se
		}
	}

	return nil
}

func (m *MySQLReader) Close() error {
	m.binlogSyncer.Close()
	return nil
}

func (m *MySQLReader) clearTables() {
	m.tables = make(map[string]*msql.Table)
}

func (m *MySQLReader) skipRowEvent(schema string, table string) bool {
	if m.mysqlContext.Database != nil || m.mysqlContext.Table != nil {
		table = strings.ToLower(table)
		//if table in tartget Table, do this event
		for _, d := range m.mysqlContext.Table {
			if m.matchString(d.Schema, schema) && m.matchString(d.Name, table) {
				return false
			}
		}

		//if schema in target DB, do this event
		if m.matchDB(m.mysqlContext.Database, schema) && len(m.mysqlContext.Database) > 0 {
			return false
		}

		return true
	}
	return false
}

func (m *MySQLReader) skipQueryEvent(sql string, schema string) bool {
	sql = strings.ToUpper(sql)

	if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE ON") {
		return true
	}

	if strings.HasPrefix(sql, "FLUSH PRIVILEGES") {
		return true
	}

	return false
}

func (m *MySQLReader) skipQueryDDL(sql string, schema string) bool {
	tb, err := msql.ParserDDLTableName(sql)
	if err != nil {
		m.logger.Printf("[ERR] client: get table failure %s %s", sql, err)
	}

	if err == nil && (m.mysqlContext.Database != nil || m.mysqlContext.Table != nil) {
		//if table in target Table, do this sql
		if tb.Schema == "" {
			tb.Schema = schema
		}
		if m.matchTable(m.mysqlContext.Table, tb) {
			return false
		}

		// if  schema in target DB, do this sql
		if m.matchDB(m.mysqlContext.Database, tb.Schema) {
			return false
		}
		return true
	}
	return false
}

func (m *MySQLReader) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if m.matchString(b, a) {
			return true
		}
	}
	return false
}

func (m *MySQLReader) matchString(pattern string, t string) bool {
	if re, ok := m.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (m *MySQLReader) matchTable(patternTBS []msql.TableName, tb msql.TableName) bool {
	for _, ptb := range patternTBS {
		retb, oktb := m.reMap[ptb.Name]
		redb, okdb := m.reMap[ptb.Schema]

		if oktb && okdb {
			if redb.MatchString(tb.Schema) && retb.MatchString(tb.Name) {
				return true
			}
		}
		if oktb {
			if retb.MatchString(tb.Name) && tb.Schema == ptb.Schema {
				return true
			}
		}
		if okdb {
			if redb.MatchString(tb.Schema) && tb.Name == ptb.Name {
				return true
			}
		}
		//create database or drop database
		if tb.Name == "" {
			if tb.Schema == ptb.Schema {
				return true
			}
		}
		if ptb == tb {
			return true
		}
	}
	return false
}

func (m *MySQLReader) getTable(schema string, table string) (*msql.Table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := m.tables[key]
	if ok {
		return value, nil
	}

	t, err := m.getTableFromDB(m.db, schema, table)
	if err != nil {
		return nil, err
	}

	m.tables[key] = t
	return t, nil
}

func (m *MySQLReader) getTableFromDB(db *gosql.DB, schema string, name string) (*msql.Table, error) {
	table := &msql.Table{}
	table.Schema = schema
	table.Name = name

	err := m.getTableColumns(db, table)
	if err != nil {
		return nil, err
	}

	err = m.getTableIndex(db, table)
	if err != nil {
		return nil, err
	}

	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func (m *MySQLReader) getTableColumns(db *gosql.DB, table *msql.Table) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf("schema/table is empty")
	}

	query := fmt.Sprintf("show columns from %s.%s", table.Schema, table.Name)
	rows, err := msql.QuerySQL(db, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return err
	}

	idx := 0
	for rows.Next() {
		datas := make([]gosql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		column := &msql.Column{}
		column.Idx = idx
		column.Name = string(datas[0])

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(datas[1])), "unsigned") {
			column.Unsigned = true
		}

		table.Columns = append(table.Columns, column)
		idx++
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}

func (m *MySQLReader) getTableIndex(db *gosql.DB, table *msql.Table) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf("schema/table is empty")
	}

	query := fmt.Sprintf("show index from %s.%s", table.Schema, table.Name)
	rows, err := msql.QuerySQL(db, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return err
	}

	var keyName string
	var columns []string
	for rows.Next() {
		datas := make([]gosql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		nonUnique := string(datas[1])
		if nonUnique == "0" {
			if keyName == "" {
				keyName = string(datas[2])
			} else {
				if keyName != string(datas[2]) {
					break
				}
			}

			columns = append(columns, string(datas[4]))
		}
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	table.IndexColumns = msql.FindColumns(table.Columns, columns)
	return nil
}
