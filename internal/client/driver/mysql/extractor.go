package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	stan "github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"github.com/hashicorp/consul/lib"
	ubinlog "udup/internal/client/driver/mysql/binlog"
	usql "udup/internal/client/driver/mysql/sql"
	uconf "udup/internal/config"
	"udup/internal/models"
)

const (
	ReconnectStreamerSleepSeconds = 10
	EventsChannelBufferSize       = 200
)

type Extractor struct {
	subject                  string
	cfg                      *uconf.MySQLDriverConfig
	initialBinlogCoordinates *ubinlog.BinlogCoordinates
	tables                   map[string]*usql.Table
	db                       *sql.DB
	tb                       *ubinlog.TxBuilder
	currentFde               string
	currentSqlB64            *bytes.Buffer
	bp                       *ubinlog.BinlogParser
	stanConn                 stan.Conn

	start    time.Time
	lastTime time.Time

	logger *log.Logger
	waitCh chan error
}

func NewExtractor(subject string, cfg *uconf.MySQLDriverConfig, logger *log.Logger) *Extractor {
	return &Extractor{
		subject: subject,
		cfg:     cfg,
		tables:  make(map[string]*usql.Table),
		waitCh:  make(chan error, 1),
		logger:  logger,
	}
}

func (e *Extractor) Run() error {
	e.start = time.Now()
	e.lastTime = e.start
	e.logger.Printf("[INFO] mysql.extractor: extract binlog events from %v", e.cfg.Dsn.String())

	if err := e.initiateTxBuilder(); err != nil {
		return err
	}

	if err := e.initDBConnections(); err != nil {
		return err
	}

	if err := e.initNatsPubClient(); err != nil {
		return err
	}

	go func() {
		for tx := range e.tb.TxChan {
			msg, err := Encode(tx)
			if err != nil {
				e.waitCh <- err
			}

			if err := e.stanConn.Publish(e.subject, msg); err != nil {
				e.waitCh <- err
			}
		}
	}()

	go e.bp.StreamEvents(e.tb.EvtChan)
	return nil
}

func (h *Extractor) WaitCh() chan error {
	return h.waitCh
}

func (h *Extractor) GtidCh() chan string {
	return nil
}

func (e *Extractor) Stats() (*models.TaskStatistics, error) {
	now := time.Now()
	// Table Related Stats
	tbs := &models.TableStats{
		InsertCount: e.tb.InsertCount.Get(),
		UpdateCount: e.tb.UpdateCount.Get(),
		DelCount:    e.tb.DeleteCount.Get(),
	}

	// Delay Related Stats
	dc := &models.DelayCount{
		Num:  1,
		Time: 60,
	}

	// Delay Related Stats
	ths := &models.ThroughputStat{
		Num:  1,
		Time: 60,
	}
	taskResUsage := models.TaskStatistics{
		Stats: &models.Stats{
			TableStats:     tbs,
			DelayCount:     dc,
			ThroughputStat: ths,
		},
		Timestamp: now.UTC().UnixNano(),
	}
	return &taskResUsage, nil
}

func (e *Extractor) ID() string {
	id := uconf.DriverCtx{
		DriverConfig: &uconf.MySQLDriverConfig{
			ReplicateDoDb: e.cfg.ReplicateDoDb,
			Gtid:          gtid,
			NatsAddr:      e.cfg.NatsAddr,
			WorkerCount:   e.cfg.WorkerCount,
			Dsn:           e.cfg.Dsn,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		e.logger.Printf("[ERR] mysql.extractor: failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *Extractor) initiateTxBuilder() error {
	e.tb = &ubinlog.TxBuilder{
		Logger:  e.logger,
		Cfg:     e.cfg,
		WaitCh:  e.waitCh,
		EvtChan: make(chan *ubinlog.BinlogEvent, EventsChannelBufferSize),
		TxChan:  make(chan *ubinlog.Transaction_t, 100),
		ReMap:   make(map[string]*regexp.Regexp),
	}
	go e.tb.Run()
	return nil
}

func (e *Extractor) initDBConnections() (err error) {
	if e.db, err = usql.CreateDB(e.cfg.Dsn); err != nil {
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

	return nil
}

func (e *Extractor) retryIntv(base time.Duration) time.Duration {
	return base + lib.RandomStagger(base)
}

// initBinlogParser creates and connects the reader: we hook up to a MySQL server as a replica
func (e *Extractor) initBinlogParser(binlogCoordinates *ubinlog.BinlogCoordinates) error {
	binlogParser, err := ubinlog.NewBinlogParser(e.cfg, e.logger)
	if err != nil {
		return err
	}
	binlogParser.WaitCh = e.waitCh
	if err := binlogParser.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		return err
	}
	e.bp = binlogParser
	return nil
}

func (e *Extractor) initNatsPubClient() (err error) {
	sc, err := stan.Connect(uconf.DefaultClusterID, uuid.NewV4().String(), stan.NatsURL(fmt.Sprintf("nats://%s", e.cfg.NatsAddr)), stan.ConnectWait(DefaultConnectWait))
	if err != nil {
		e.logger.Printf("[ERR] mysql.extractor: Can't connect nats server %v.Make sure a NATS Streaming Server is running.%v", fmt.Sprintf("nats://%s", e.cfg.NatsAddr), err)
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

func (e *Extractor) Shutdown() error {
	e.stanConn.Close()

	if err := usql.CloseDBs(e.db); err != nil {
		return err
	}

	e.logger.Printf("[INFO] mysql.extractor: closed streamer connection.")
	return nil
}
