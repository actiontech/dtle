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
	/*seconds := now.Unix() - e.lastTime.Unix()
	totalSeconds := now.Unix() - e.start.Unix()
	last := e.tb.LastCount.Get()
	total := e.tb.Count.Get()

	tps, totalTps := int64(0), int64(0)
	if seconds > 0 {
		tps = (total - last) / seconds
		totalTps = total / totalSeconds
	}
	e.logger.Printf("[DEBUG] mysql.extractor: total events = %d, insert = %d, update = %d, delete = %d, total tps = %d, recent tps = %d .",
		e.tb.Count.Get(), e.tb.InsertCount.Get(), e.tb.UpdateCount.Get(), e.tb.DeleteCount.Get(), totalTps, tps)

	e.tb.LastCount.Set(total)
	e.lastTime = time.Now()*/
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

	/*elapsedTime := this.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&this.migrationContext.RowsEstimate) + atomic.LoadInt64(&this.migrationContext.RowsDeltaEstimate)
	if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}
	var progressPct float64
	if rowsEstimate == 0 {
		progressPct = 100.0
	} else {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}

	var etaSeconds float64 = math.MaxFloat64
	eta := "N/A"
	if progressPct >= 100.0 {
		eta = "due"
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := this.migrationContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "due"
		}
	}

	state := "migrating"
	if atomic.LoadInt64(&this.migrationContext.CountingRowsFlag) > 0 && !this.migrationContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} else if isThrottled, throttleReason, _ := this.migrationContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}

	shouldPrintStatus := false
	if rule == HeuristicPrintStatusRule {
		if elapsedSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if elapsedSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if this.migrationContext.TimeSincePointOfInterest().Seconds() <= 60 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else {
			shouldPrintStatus = (elapsedSeconds%30 == 0)
		}
	} else {
		// Not heuristic
		shouldPrintStatus = true
	}
	if !shouldPrintStatus {
		return nil,nil
	}

	currentBinlogCoordinates := *e.GetCurrentBinlogCoordinates()

	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; State: %s; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
		currentBinlogCoordinates,
		state,
		eta,
	)
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		status,
	)
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, status)

	if elapsedSeconds%60 == 0 {
		this.hooksExecutor.onStatus(status)
	}
	return nil, nil*/
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
