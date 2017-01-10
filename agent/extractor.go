package agent

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/ngaut/log"
	"github.com/outbrain/golib/sqlutils"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	usql "udup/agent/mysql"
	uconf "udup/config"
)

const (
	EventsChannelBufferSize = 1
)

type Extractor struct {
	cfg            *uconf.Config
	binlogSyncer   *replication.BinlogSyncer
	binlogStreamer *replication.BinlogStreamer
	tables         map[string]*usql.Table
	db             *sql.DB
	eventsChannel  chan *usql.StreamEvent
	reMap          map[string]*regexp.Regexp

	natsConn *nats.Conn
}

func NewExtractor(cfg *uconf.Config) *Extractor {
	return &Extractor{
		cfg:           cfg,
		tables:        make(map[string]*usql.Table),
		eventsChannel: make(chan *usql.StreamEvent, EventsChannelBufferSize),
		reMap:         make(map[string]*regexp.Regexp),
	}
}

func (e *Extractor) initiateExtractor() error {
	log.Infof("Extract binlog events from the datasource :%v", e.cfg.Extract.ConnCfg)
	time.Sleep(5 * time.Second)

	if err := e.initDBConnections(); err != nil {
		return err
	}

	log.Infof("Beginning streaming")
	e.genRegexMap()
	err := e.streamEvents()
	if err != nil {
		return err
	}

	return nil
}

func (e *Extractor) initDBConnections() (err error) {
	if e.db, _, err = sqlutils.GetDB(e.cfg.Extract.ConnCfg.GetDBUri()); err != nil {
		return err
	}

	if err = e.initBinlogSyncer(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) genRegexMap() {
	for _, db := range e.cfg.ReplicateDoDb {
		if db[0] != '~' {
			continue
		}
		if _, ok := e.reMap[db]; !ok {
			e.reMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, tb := range e.cfg.ReplicateDoTable {
		if tb.Name[0] == '~' {
			if _, ok := e.reMap[tb.Name]; !ok {
				e.reMap[tb.Name] = regexp.MustCompile(tb.Name[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := e.reMap[tb.Schema]; !ok {
				e.reMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}
}

func (e *Extractor) initBinlogSyncer() (err error) {
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(e.cfg.Extract.ServerID),
		Flavor:          "mysql",
		Host:            e.cfg.Extract.ConnCfg.Host,
		Port:            uint16(e.cfg.Extract.ConnCfg.Port),
		User:            e.cfg.Extract.ConnCfg.User,
		Password:        e.cfg.Extract.ConnCfg.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}
	e.binlogSyncer = replication.NewBinlogSyncer(&cfg)
	return nil
}

func (e *Extractor) streamEvents() (err error) {
	nc, err := nats.Connect(fmt.Sprintf("nats://%s", e.cfg.NatsAddr))
	if err != nil {
		return err
	}

	c, _ := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	e.natsConn = nc

	go func() {
		for event := range e.eventsChannel {
			if event != nil {
				if err := c.Publish("subject", event); err != nil {
					log.Infof("err:%v", err)
					e.cfg.PanicAbort <- err
				}
			}
		}
	}()

	pos := gomysql.Position{"", uint32(4)}
	// Start sync with sepcified binlog file and position
	if e.binlogStreamer, err = e.binlogSyncer.StartSync(pos); err != nil {
		return err
	}

	for {
		event, err := e.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			log.Infof("rotate binlog to %v:%v", string(ev.NextLogName), uint32(ev.Position))
		case *replication.RowsEvent:
			table := &usql.Table{}
			if table, err = e.getTable(string(ev.Table.Schema), string(ev.Table.Table)); err != nil {
				return err
			}

			var (
				sqls []string
				keys []string
				args [][]interface{}
			)
			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				sqls, keys, args, err = usql.BuildDMLInsertQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("build insert query failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := usql.NewStreamEvent(usql.Insert, sqls[i], args[i], keys[i], true, pos)
					e.eventsChannel <- se
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, keys, args, err = usql.BuildDMLUpdateQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("build update query failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := usql.NewStreamEvent(usql.Update, sqls[i], args[i], keys[i], true, pos)
					e.eventsChannel <- se
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, keys, args, err = usql.BuildDMLDeleteQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("build delete query failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := usql.NewStreamEvent(usql.Del, sqls[i], args[i], keys[i], true, pos)
					e.eventsChannel <- se
				}
			}
		case *replication.QueryEvent:
			sql := string(ev.Query)
			if e.skipQueryEvent(sql, string(ev.Schema)) {
				log.Warnf("skip query %s,schema:%s", sql, string(ev.Schema))
				continue
			}

			sqls, ok, err := usql.ResolveDDLSQL(sql)
			if err != nil {
				return fmt.Errorf("parse query event failed: %v", err)
			}
			if !ok {
				continue
			}

			pos.Pos = event.Header.LogPos
			for _, sql := range sqls {
				if e.skipQueryDDL(sql, string(ev.Schema)) {
					log.Warnf("skip query ddl %s,schema:%s", sql, ev.Schema)
					continue
				}

				sql, err = usql.GenDDLSQL(sql, string(ev.Schema))
				if err != nil {
					return err
				}

				se := usql.NewStreamEvent(usql.Ddl, sql, nil, "", false, pos)
				e.eventsChannel <- se

				e.clearTables()
			}
		case *replication.XIDEvent:
			pos.Pos = event.Header.LogPos
			se := usql.NewStreamEvent(usql.Xid, "", nil, "", false, pos)
			e.eventsChannel <- se
		}
	}
	return nil
}

func (e *Extractor) clearTables() {
	e.tables = make(map[string]*usql.Table)
}

func (e *Extractor) getTableFromDB(db *sql.DB, schema string, name string) (*usql.Table, error) {
	table := &usql.Table{}
	table.Schema = schema
	table.Name = name

	err := e.getTableColumns(db, table)
	if err != nil {
		return nil, err
	}

	err = e.getTableIndex(db, table)
	if err != nil {
		return nil, err
	}

	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func (e *Extractor) getTableColumns(db *sql.DB, table *usql.Table) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf("schema/table is empty")
	}

	query := fmt.Sprintf("show columns from %s.%s", table.Schema, table.Name)
	rows, err := usql.QuerySQL(db, query)
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
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		column := &usql.Column{}
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

func (e *Extractor) getTableIndex(db *sql.DB, table *usql.Table) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf("schema/table is empty")
	}

	query := fmt.Sprintf("show index from %s.%s", table.Schema, table.Name)
	rows, err := usql.QuerySQL(db, query)
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
		datas := make([]sql.RawBytes, len(rowColumns))
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

	table.IndexColumns = usql.FindColumns(table.Columns, columns)
	return nil
}

func (e *Extractor) getTable(schema string, table string) (*usql.Table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := e.tables[key]
	if ok {
		return value, nil
	}

	t, err := e.getTableFromDB(e.db, schema, table)
	if err != nil {
		return nil, err
	}

	e.tables[key] = t
	return t, nil
}

func (e *Extractor) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if e.matchString(b, a) {
			return true
		}
	}
	return false
}

func (e *Extractor) matchString(pattern string, t string) bool {
	if re, ok := e.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (e *Extractor) matchTable(patternTBS []uconf.TableName, tb uconf.TableName) bool {
	for _, ptb := range patternTBS {
		retb, oktb := e.reMap[ptb.Name]
		redb, okdb := e.reMap[ptb.Schema]

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

func (e *Extractor) skipRowEvent(schema string, table string) bool {
	if e.cfg.ReplicateDoTable != nil || e.cfg.ReplicateDoDb != nil {
		table = strings.ToLower(table)
		//if table in tartget Table, do this event
		for _, d := range e.cfg.ReplicateDoTable {
			if e.matchString(d.Schema, schema) && e.matchString(d.Name, table) {
				return false
			}
		}

		//if schema in target DB, do this event
		if e.matchDB(e.cfg.ReplicateDoDb, schema) && len(e.cfg.ReplicateDoDb) > 0 {
			return false
		}

		return true
	}
	return false
}

func (e *Extractor) skipQueryEvent(sql string, schema string) bool {
	sql = strings.ToUpper(sql)

	if strings.HasPrefix(sql, "CREATE USER") {
		return true
	}

	if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE ON") {
		return true
	}

	if strings.HasPrefix(sql, "FLUSH PRIVILEGES") {
		return true
	}

	return false
}

func (e *Extractor) skipQueryDDL(sql string, schema string) bool {
	tb, err := usql.ParserDDLTableName(sql)
	if err != nil {
		log.Warnf("[get table failure]:%s %s", sql, err)
	}

	if err == nil && (e.cfg.ReplicateDoTable != nil || e.cfg.ReplicateDoDb != nil) {
		//if table in target Table, do this sql
		if tb.Schema == "" {
			tb.Schema = schema
		}
		if e.matchTable(e.cfg.ReplicateDoTable, tb) {
			return false
		}

		// if  schema in target DB, do this sql
		if e.matchDB(e.cfg.ReplicateDoDb, tb.Schema) {
			return false
		}
		return true
	}
	return false
}

func (e *Extractor) Shutdown() error {
	usql.CloseDBs(e.db)

	if e.binlogSyncer != nil {
		e.binlogSyncer.Close()
		e.binlogSyncer = nil
	}

	close(e.eventsChannel)

	e.natsConn.Close()
	return nil
}
