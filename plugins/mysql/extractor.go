package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"regexp"
	"strings"
	"time"

	stan "github.com/nats-io/go-nats-streaming"
	"github.com/ngaut/log"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	usql "udup/plugins/mysql/sql"
	uconf "udup/config"
)

const (
	EventsChannelBufferSize = 1
)

type Extractor struct {
	cfg            *uconf.DriverConfig
	binlogSyncer   *replication.BinlogSyncer
	binlogStreamer *replication.BinlogStreamer
	tables         map[string]*usql.Table
	db             *sql.DB
	eventsChannel  chan *usql.StreamEvent
	reMap          map[string]*regexp.Regexp

	stanConn stan.Conn
}

func NewExtractor(cfg *uconf.DriverConfig) *Extractor {
	return &Extractor{
		cfg:           cfg,
		tables:        make(map[string]*usql.Table),
		eventsChannel: make(chan *usql.StreamEvent, EventsChannelBufferSize),
		reMap:         make(map[string]*regexp.Regexp),
	}
}

func (e *Extractor) InitiateExtractor() error {
	log.Infof("Extract binlog events from the datasource :%v", e.cfg.ConnCfg)
	time.Sleep(10 * time.Second)

	if err := e.initDBConnections(); err != nil {
		return err
	}

	if err := e.initNatsPubClient(); err != nil {
		return err
	}
	go func() {
		for event := range e.eventsChannel {
			if event != nil {
				msg, err := Encode(event)
				if err != nil {
					//e.cfg.PanicAbort <- err
				}
				if err := e.stanConn.Publish("subject", msg); err != nil {
					log.Infof("Publish err:%v", err)
					//e.cfg.PanicAbort <- err
				}
			}
		}
	}()
	e.genRegexMap()
	/*if err := e.copyRows(); err != nil {
		return err
	}*/
	log.Infof("Beginning streaming")
	if err := e.streamEvents(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) initDBConnections() (err error) {
	if e.db, err = usql.CreateDB(e.cfg.ConnCfg); err != nil {
		return err
	}
	if err = e.mysqlGTIDMode(); err != nil {
		return err
	}

	if err = e.initBinlogSyncer(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) mysqlGTIDMode() error {
	query := `SELECT @@gtid_mode`
	var gtidMode string
	if err := e.db.QueryRow(query).Scan(&gtidMode); err != nil {
		return err
	}
	if gtidMode != "ON" {
		return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
	}
	return nil
}

func (e *Extractor) masterStatus() (rowMap usql.RowMap) {
	rowMap = nil
	query := `SHOW MASTER STATUS`
	usql.QueryRowsMap(e.db, query, func(m usql.RowMap) error {
		rowMap = m
		return nil
	})
	return rowMap
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
		ServerID:        uint32(e.cfg.ServerID),
		Flavor:          "mysql",
		Host:            e.cfg.ConnCfg.Host,
		Port:            uint16(e.cfg.ConnCfg.Port),
		User:            e.cfg.ConnCfg.User,
		Password:        e.cfg.ConnCfg.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}
	e.binlogSyncer = replication.NewBinlogSyncer(&cfg)
	return nil
}

func (e *Extractor) initNatsPubClient() (err error) {
	sc, err := stan.Connect("test-cluster", "pub1", stan.NatsURL(fmt.Sprintf("nats://%s", e.cfg.NatsAddr)))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, fmt.Sprintf("nats://%s", e.cfg.NatsAddr))
	}
	e.stanConn = sc
	return nil
}

// Encode
func Encode(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	enc := gob.NewEncoder(b)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

/*func (e *Extractor) copyRows() (err error) {
// ------
// STEP 0
// ------
// Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
// which is why we explicitly set it here.
//
// With REPEATABLE READ, all SELECT queries within the scope of a transaction (which we don't yet have) will read
// from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
// consistent also with respect to each other.
//
// See: https://dev.mysql.com/doc/refman/5.7/en/set-transaction.html
// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html
// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
log.Infof("Step 0: disabling autocommit and enabling repeatable read transactions")
if _, err := usql.ExecNoPrepare(e.db, `SET AUTOCOMMIT = 0`); err != nil {
	return err
}
if _, err := usql.ExecNoPrepare(e.db, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
	return err
}
// ------
// STEP 1
// ------
// First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
// See http://dev.mysql.com/doc/refman/5.7/en/commit.html
log.Infof("Step 1: start transaction with consistent snapshot")
if _, err := usql.ExecNoPrepare(e.db, `START TRANSACTION WITH CONSISTENT SNAPSHOT`); err != nil {
	return err
}
// ------
// STEP 2
// ------
// Obtain read lock on all tables. This statement closes all open tables and locks all tables
// for all databases with a global read lock, and it prevents ALL updates while we have this lock.
// It also ensures that everything we do while we have this lock will be consistent.
lockAcquired := time.Millisecond
log.Infof("Step 2: flush and obtain global read lock (preventing writes to database)")
if _, err := usql.ExecNoPrepare(e.db, `FLUSH TABLES WITH READ LOCK`); err != nil {
	return err
}
// ------
// STEP 3
// ------
// Obtain the binlog position and update the SourceInfo in the context. This means that all source records generated
// as part of the snapshot will contain the binlog position of the snapshot.
log.Infof("Step 3: read binlog position of MySQL master")
foundMasterStatus := false
err = usql.QueryRowsMap(e.db, "SHOW MASTER STATUS", func(m usql.RowMap) error {
	//source.setBinlogStartPoint(binlogFilename, binlogPosition);
	gtidSet := m["Executed_Gtid_Set"]
	//source.setCompletedGtidSet(gtidSet);
	log.Infof("\t using binlog '%s' at position '%s' and gtid '%s'", m["File"], m["Position"],
		gtidSet)
	//source.startSnapshot();
	foundMasterStatus = true
	return nil
})
if err != nil {
	return err
}
if !foundMasterStatus {
	return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
}

// ------
// STEP 6
// ------
// Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
// First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
log.Infof("Step 6: generating DROP and CREATE statements to reflect current database schemas:")
for _, b := range e.cfg.ReplicateDoDb {
	se := usql.NewStreamEvent(usql.Ddl,
		fmt.Sprintf(`DROP DATABASE IF EXISTS %s`,
			usql.EscapeName(b)),
		nil,
		"",
		true)
	e.eventsChannel <- se
	se = usql.NewStreamEvent(usql.Ddl,
		fmt.Sprintf(`CREATE DATABASE %s`, usql.EscapeName(b)),
		nil,
		"",
		true)
	e.eventsChannel <- se
	for _, d := range e.cfg.ReplicateDoTable {
		se = usql.NewStreamEvent(usql.Ddl,
			fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
				usql.EscapeName(d.Schema),
				usql.EscapeName(d.Name)),
			nil,
			"",
			true)
		e.eventsChannel <- se
		createTableStatement := ""
		err = e.db.QueryRow(fmt.Sprintf(`SHOW CREATE TABLE %s.%s`,
			usql.EscapeName(d.Schema),
			usql.EscapeName(d.Name))).Scan(&createTableStatement)
		if err != nil {
			log.Infof("---err:%v", err)
			return err
		}
		se = usql.NewStreamEvent(usql.Ddl,
			createTableStatement,
			nil,
			"",
			true)
		e.eventsChannel <- se
	}
}
// ------
// STEP 7
// ------
// We are doing minimal blocking, then we should release the read lock now. All subsequent SELECT
// should still use the MVCC snapshot obtained when we started our transaction (since we started it
// "...with consistent snapshot"). So, since we're only doing very simple SELECT without WHERE predicates,
// we can release the lock now ...
log.Infof("Step 7: releasing global read lock to enable MySQL writes")
if _, err := usql.ExecNoPrepare(e.db, `UNLOCK TABLES`); err != nil {
	return err
}
//unlocked := true
lockReleased := time.Millisecond
log.Infof("Step 7: blocked writes to MySQL for a total of %v", lockReleased-lockAcquired)

// ------
// STEP 8
// ------
// Use a buffered blocking consumer to buffer all of the records, so that after we copy all of the tables
// and produce events we can update the very last event with the non-snapshot offset ...
// Dump all of the tables and generate source records ...
log.Infof("Step 8: scanning contents of %d tables", len(e.cfg.ReplicateDoTable))

for _, d := range e.cfg.ReplicateDoTable {
	if err := e.CountTableRows(d.Schema, d.Name); err != nil {
		return err
	}

	rows, err := e.db.Query(`SELECT * FROM %s.%s`, d.Schema, d.Name)
	if err != nil {
		return err
	}
	log.Infof("---row:%v", rows)
*/ /*for rows.Next() {
	if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
		return  err
	}
}*/ /*
	}

	// ------
	// STEP 9
	// ------
	// Release the read lock if we have not yet done so ...
*/ /*if (!unlocked) {
	log.Infof("Step 9: releasing global read lock to enable MySQL writes")
	if _, err := usql.ExecNoPrepare(e.db, `UNLOCK TABLES`); err != nil {
		return err
	}
	unlocked = true;
	lockReleased = time.Millisecond
	log.Infof("Writes to MySQL prevented for a total of {}", lockReleased - lockAcquired);
}*/ /*
	log.Infof("Step 9: committing transaction")
	if _, err := usql.ExecNoPrepare(e.db, `COMMIT`); err != nil {
		return err
	}

	return nil
}

// CountTableRows counts exact number of rows on the original table
func (e *Extractor) CountTableRows(db, table string) error {
	log.Infof("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	query := fmt.Sprintf(`SELECT COUNT(*) AS ROWS FROM %s.%s`, db, table)
	var rowsEstimate int64
	if err := e.db.QueryRow(query).Scan(&rowsEstimate); err != nil {
		return err
	}

	log.Infof("Exact number of rows via COUNT: %d", rowsEstimate)

	return nil
}*/

func (e *Extractor) streamEvents() (err error) {
	rowMap := e.masterStatus()
	if rowMap == nil {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out.")
	}

	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	executedGtidSet := strings.Split(rowMap["Executed_Gtid_Set"].String, ":")
	gtidSet, err := gomysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:1", executedGtidSet[0]))
	if err != nil {
		return err
	}
	// StartSyncGTID starts syncing from the `gset` GTIDSet.
	if e.binlogStreamer, err = e.binlogSyncer.StartSyncGTID(gtidSet); err != nil {
		return err
	}

	for {
		event, err := e.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		//event.Dump(os.Stdout)

		switch ev := event.Event.(type) {
		case *replication.GTIDEvent:
			//log.Infof("gtid %v", ev)
			u, _ := uuid.FromBytes(ev.SID)
			se := usql.NewStreamEvent(usql.Gtid,
				fmt.Sprintf(`SET GTID_NEXT='%s:%d'`, u.String(), ev.GNO),
				nil,
				"",
				false)
			e.eventsChannel <- se
		//case *replication.RotateEvent:
		case *replication.RowsEvent:
			table := &usql.Table{}
			if e.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table)) {
				log.Warnf("skip RowsEvent with db:%s table:%s", ev.Table.Schema, ev.Table.Table)
				continue
			}
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
					se := usql.NewStreamEvent(usql.Insert,
						sqls[i],
						args[i],
						keys[i],
						true)
					e.eventsChannel <- se
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, keys, args, err = usql.BuildDMLUpdateQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("build update query failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := usql.NewStreamEvent(usql.Update,
						sqls[i],
						args[i],
						keys[i],
						true)
					e.eventsChannel <- se
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, keys, args, err = usql.BuildDMLDeleteQuery(table.Schema, table.Name, ev.Rows, table.Columns, table.IndexColumns)
				if err != nil {
					return fmt.Errorf("build delete query failed: %v, schema: %s, table: %s", err, table.Schema, table.Name)
				}

				for i := range sqls {
					se := usql.NewStreamEvent(usql.Del,
						sqls[i],
						args[i],
						keys[i],
						true)
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

			for _, sql := range sqls {
				if e.skipQueryDDL(sql, string(ev.Schema)) {
					log.Warnf("skip query ddl %s,schema:%s", sql, ev.Schema)
					continue
				}

				sql, err = usql.GenDDLSQL(sql, string(ev.Schema))
				if err != nil {
					return fmt.Errorf("gen event failed: %v", err)
				}

				se := usql.NewStreamEvent(usql.Ddl,
					sql,
					nil,
					"",
					false)
				e.eventsChannel <- se

				e.clearTables()
			}
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

	if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE ON") {
		return true
	}

	if strings.HasPrefix(sql, "BEGIN") {
		return true
	}

	if strings.HasPrefix(sql, "COMMIT") {
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

	e.binlogSyncer.Close()

	close(e.eventsChannel)

	e.stanConn.Close()
	return nil
}
