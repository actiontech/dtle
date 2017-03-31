package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"

	uconf "udup/config"
	ulog "udup/logger"
	ubinlog "udup/plugins/mysql/binlog"
	usql "udup/plugins/mysql/sql"
)

const (
	ReconnectStreamerSleepSeconds = 10
)

type Extractor struct {
	subject                  string
	cfg                      *uconf.DriverConfig
	initialBinlogCoordinates *ubinlog.BinlogCoordinates
	tables                   map[string]*usql.Table
	db                       *sql.DB
	tb                       *ubinlog.TxBuilder
	currentFde               string
	currentSqlB64            *bytes.Buffer
	bp                       *ubinlog.BinlogParser

	stanConn stan.Conn
}

func NewExtractor(cfg *uconf.DriverConfig, subject string) *Extractor {
	return &Extractor{
		subject: subject,
		cfg:     cfg,
		tables:  make(map[string]*usql.Table),
	}
}

func (e *Extractor) InitiateExtractor() error {
	ulog.Logger.WithFields(logrus.Fields{
		"job":    e.subject,
		"config": e.cfg.ConnCfg.String(),
	}).Info("extractor: Extract binlog events")

	if err := e.initDBConnections(); err != nil {
		return err
	}

	if err := e.initNatsPubClient(); err != nil {
		return err
	}

	if err := e.initiateTxBuilder(); err != nil {
		return err
	}

	go func() {
		ulog.Logger.WithFields(logrus.Fields{
			"job": e.subject,
		}).Info("extractor: Beginning streaming")
		if err := e.streamEvents(); err != nil {
			e.cfg.ErrCh <- err
		}
	}()

	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *Extractor) initiateTxBuilder() error {
	e.tb = ubinlog.NewTxBuilder(e.cfg)
	go e.tb.Run()
	return nil
}

func (e *Extractor) initDBConnections() (err error) {
	if e.db, err = usql.CreateDB(e.cfg.ConnCfg); err != nil {
		return err
	}
	if err = e.mysqlGTIDMode(); err != nil {
		return err
	}

	if err := e.readCurrentBinlogCoordinates(); err != nil {
		return err
	}

	if err = e.initBinlogParser(e.initialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) mysqlGTIDMode() error {
	query := `SELECT @@GTID_MODE`
	var gtidMode string
	if err := e.db.QueryRow(query).Scan(&gtidMode); err != nil {
		return err
	}
	if gtidMode != "ON" {
		return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
	}
	return nil
}

func (e *Extractor) mysqlServerUUID() string {
	query := `SELECT @@SERVER_UUID`
	var server_uuid string
	if err := e.db.QueryRow(query).Scan(&server_uuid); err != nil {
		ulog.Logger.Errorf("error to SELECT @@SERVER_UUID:%v", err)
		return ""
	}
	return server_uuid
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (e *Extractor) readCurrentBinlogCoordinates() error {
	if e.cfg.Gtid != "" {
		gtidSet, err := gomysql.ParseMysqlGTIDSet(e.cfg.Gtid)
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = &ubinlog.BinlogCoordinates{
			GtidSet: gtidSet,
		}
	} else {
		server_uuid := e.mysqlServerUUID()
		gtidSet, err := gomysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:1", server_uuid))
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = &ubinlog.BinlogCoordinates{
			GtidSet: gtidSet,
		}
	}

	ulog.Logger.WithFields(logrus.Fields{
		"coordinates": *e.initialBinlogCoordinates,
	}).Debug("extractor: Streamer binlog coordinates")
	return nil
}

// initBinlogParser creates and connects the reader: we hook up to a MySQL server as a replica
func (e *Extractor) initBinlogParser(binlogCoordinates *ubinlog.BinlogCoordinates) error {
	binlogParser, err := ubinlog.NewBinlogParser(e.cfg)
	if err != nil {
		return err
	}
	if err := binlogParser.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		return err
	}
	e.bp = binlogParser
	return nil
}

func (e *Extractor) initNatsPubClient() (err error) {
	sc, err := stan.Connect(uconf.DefaultClusterID, uuid.NewV4().String(), stan.NatsURL(fmt.Sprintf("nats://%s", e.cfg.NatsAddr)), stan.ConnectWait(DefaultConnectWait))
	if err != nil {
		ulog.Logger.WithFields(logrus.Fields{
			"err":         err,
			"nats_server": fmt.Sprintf("nats://%s", e.cfg.NatsAddr),
		}).Error("extractor: Can't connect nats server.\nMake sure a NATS Streaming Server is running.")
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

func (e *Extractor) GetCurrentBinlogCoordinates() *ubinlog.BinlogCoordinates {
	return e.bp.GetCurrentBinlogCoordinates()
}

func (e *Extractor) GetReconnectBinlogCoordinates() *ubinlog.BinlogCoordinates {
	return &ubinlog.BinlogCoordinates{LogFile: e.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

func (e *Extractor) Running() bool {
	return e.cfg.Running
}

func (e *Extractor) streamEvents() error {
	go func() {
		for tx := range e.tb.TxChan {
			msg, err := Encode(tx)
			if err != nil {
				e.cfg.ErrCh <- err
			}
			if err := e.stanConn.Publish(e.subject, msg); err != nil {
				e.cfg.ErrCh <- err
			}
		}
	}()
	// The next should block and execute forever, unless there's a serious error
	var successiveFailures int64
	var lastAppliedRowsEventHint ubinlog.BinlogCoordinates
	for {
		if err := e.bp.StreamEvents(e.Running, e.tb.EvtChan); err != nil {
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)
			if !e.Running() {
				return nil
			}
			ulog.Logger.WithField("job", e.subject).WithError(err).Info("extractor: StreamEvents encountered unexpected error")

			// See if there's retry overflow
			if e.bp.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}
			if successiveFailures > e.cfg.MaxRetries {
				ulog.Logger.WithField("job", e.subject).
					WithError(err).Errorf(
					"extractor: %d successive failures in streamer reconnect at coordinates %+v",
					successiveFailures, e.GetReconnectBinlogCoordinates())
				e.cfg.ErrCh <- err
			}

			// Reposition at same binlog file.
			lastAppliedRowsEventHint = e.bp.LastAppliedRowsEventHint

			ulog.Logger.WithFields(logrus.Fields{
				"job": e.subject,
			}).Infof("extractor: Reconnecting... Will resume at %+v", lastAppliedRowsEventHint)

			if err := e.initBinlogParser(e.GetReconnectBinlogCoordinates()); err != nil {
				ulog.Logger.Errorf(err.Error())
			}
			e.bp.LastAppliedRowsEventHint = lastAppliedRowsEventHint
		}
	}
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

func (e *Extractor) Shutdown() error {
	if !e.Running() {
		return nil
	}

	if e.bp != nil {
		e.bp.Close()
	}
	e.stanConn.Close()

	err := usql.CloseDBs(e.db)
	if err != nil {
		return err
	}
	e.cfg.Running = false

	ulog.Logger.WithFields(logrus.Fields{
		"job": e.subject,
	}).Info("extractor: Closed streamer connection.")
	return nil
}
