package mysql

import (
	"bytes"
	gosql "database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	stan "github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"

	"github.com/siddontang/go/sync2"
	ubinlog "udup/internal/client/driver/mysql/binlog"
	usql "udup/internal/client/driver/mysql/sql"
	uconf "udup/internal/config"
	"udup/internal/models"
)

var gtid string

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWait = 5 * time.Second
)

type Applier struct {
	subject     string
	cfg         *uconf.MySQLDriverConfig
	dbs         []*gosql.DB
	singletonDB *gosql.DB
	eventChans  []chan usql.StreamEvent

	eventsChannel chan *ubinlog.BinlogEvent
	currentTx     *ubinlog.Transaction_t
	txChan        chan *ubinlog.Transaction_t

	stanConn stan.Conn
	stanSub  stan.Subscription

	ddlCount    sync2.AtomicInt64
	insertCount sync2.AtomicInt64
	updateCount sync2.AtomicInt64
	deleteCount sync2.AtomicInt64
	lastCount   sync2.AtomicInt64
	count       sync2.AtomicInt64

	start    time.Time
	lastTime time.Time

	logger *log.Logger
	waitCh chan error
}

func NewApplier(subject string, cfg *uconf.MySQLDriverConfig, logger *log.Logger) *Applier {
	return &Applier{
		subject:    subject,
		cfg:        cfg,
		logger:     logger,
		eventChans: newEventChans(cfg.WorkerCount),
		txChan:     make(chan *ubinlog.Transaction_t, 100),
		waitCh:     make(chan error, 1),
	}
}

func (a *Applier) Run() error {
	a.start = time.Now()
	a.lastTime = a.start
	a.logger.Printf("[INFO] mysql.applier: apply binlog events to %v", a.cfg.Dsn.String())

	if err := a.initDBConnections(); err != nil {
		return err
	}

	if err := a.initNatSubClient(); err != nil {
		return err
	}

	if err := a.initiateStreaming(); err != nil {
		return err
	}

	// start N applier worker
	for i := 0; i < a.cfg.WorkerCount; i++ {
		go a.startApplierWorker(i, a.dbs[i])
	}
	return nil
}

func (h *Applier) WaitCh() chan error {
	return h.waitCh
}

func (a *Applier) Stats() (*models.TaskStatistics, error) {
	now := time.Now()
	/*seconds := now.Unix() - a.lastTime.Unix()
	totalSeconds := now.Unix() - a.start.Unix()
	last := a.lastCount.Get()
	total := a.count.Get()

	tps, totalTps := int64(0), int64(0)
	if seconds > 0 {
		tps = (total - last) / seconds
		totalTps = total / totalSeconds
	}
	a.logger.Printf("[DEBUG] mysql.applier: total events = %d, insert = %d, update = %d, delete = %d, total tps = %d, recent tps = %d .",
		a.count.Get(), a.insertCount.Get(), a.updateCount.Get(), a.deleteCount.Get(), totalTps, tps)

	a.lastCount.Set(total)
	a.lastTime = time.Now()*/
	// Table Related Stats
	tbs := &models.TableStats{
		InsertCount: a.insertCount.Get(),
		UpdateCount: a.updateCount.Get(),
		DelCount:    a.deleteCount.Get(),
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

func (a *Applier) addCount(tp ubinlog.OpType) {
	switch tp {
	case ubinlog.Insert:
		a.insertCount.Add(1)
	case ubinlog.Update:
		a.updateCount.Add(1)
	case ubinlog.Del:
		a.deleteCount.Add(1)
	case ubinlog.Ddl:
		a.ddlCount.Add(1)
	}
	a.count.Add(1)
}

func (a *Applier) ID() string {
	id := uconf.DriverCtx{
		DriverConfig: &uconf.MySQLDriverConfig{
			ReplicateDoDb: a.cfg.ReplicateDoDb,
			Gtid:          gtid,
			NatsAddr:      a.cfg.NatsAddr,
			WorkerCount:   a.cfg.WorkerCount,
			Dsn:           a.cfg.Dsn,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		a.logger.Printf("[ERR] mysql.applier: failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func newEventChans(count int) []chan usql.StreamEvent {
	events := make([]chan usql.StreamEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, make(chan usql.StreamEvent, 1000))
	}

	return events
}

func (a *Applier) applyTx(db *gosql.DB, transaction *ubinlog.Transaction_t) error {
	defer func() {
		_, err := usql.ExecNoPrepare(db, `COMMIT;SET GTID_NEXT='AUTOMATIC'`)
		if err != nil {
			a.waitCh <- err
		}
	}()

	_, err := usql.ExecNoPrepare(db, fmt.Sprintf(`SET GTID_NEXT='%s:%d'`, transaction.SID, transaction.GNO))
	if err != nil {
		return err
	}

	for _, query := range transaction.Query {
		if query == "" {
			continue
		}

		_, err := usql.ExecNoPrepare(db, query)
		if err != nil {
			if !usql.IgnoreDDLError(err) {
				a.logger.Printf("[ERR] mysql.applier: exec sql error: %v", err)
				return err
			} else {
				a.logger.Printf("[WARN] mysql.applier: ignore ddl error: %v", err)
			}
		}
		//a.addCount(ubinlog.Ddl)
	}

	return nil
}

func (a *Applier) startApplierWorker(i int, db *gosql.DB) {
	var lastFde string
	sid := a.mysqlServerUUID()
	for {
		select {
		case tx := <-a.txChan:
			if len(tx.Query) == 0 || sid == tx.SID {
				continue
			}

			if tx.Fde != "" && lastFde != tx.Fde {
				lastFde = tx.Fde // IMO it would comare the internal pointer first
				_, err := usql.ExecNoPrepare(db, lastFde)
				if err != nil {
					a.waitCh <- err
					break
				}
			}

			err := a.applyTx(db, tx)
			if err != nil {
				a.waitCh <- err
				break
			}

			gtid = fmt.Sprintf("%s:1-%d", tx.SID, tx.GNO)
		}
	}
}

func (a *Applier) initNatSubClient() (err error) {
	sc, err := stan.Connect(uconf.DefaultClusterID, uuid.NewV4().String(), stan.NatsURL(fmt.Sprintf("nats://%s", a.cfg.NatsAddr)), stan.ConnectWait(DefaultConnectWait))
	if err != nil {
		a.waitCh <- err
	}
	a.stanConn = sc
	return nil
}

// Decode
func Decode(data []byte, vPtr interface{}) (err error) {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(vPtr)
	return
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) initiateStreaming() error {
	sub, err := a.stanConn.Subscribe(a.subject, func(m *stan.Msg) {
		tx := &ubinlog.Transaction_t{}
		if err := Decode(m.Data, tx); err != nil {
			a.waitCh <- fmt.Errorf("subscribe err:%v", err)
		}
		a.txChan <- tx
	})

	if err != nil {
		return fmt.Errorf("unexpected error on Subscribe, got %v", err)
	}
	a.stanSub = sub
	return nil
}

func (a *Applier) initDBConnections() (err error) {
	if a.cfg.WorkerCount == 0 {
		a.cfg.WorkerCount = 1
	}
	if a.singletonDB, err = usql.CreateDB(a.cfg.Dsn); err != nil {
		return err
	}

	a.singletonDB.SetMaxOpenConns(1)
	if err := a.mysqlGTIDMode(); err != nil {
		return err
	}

	if a.dbs, err = usql.CreateDBs(a.cfg.Dsn, a.cfg.WorkerCount+1); err != nil {
		return err
	}
	return nil
}

func (a *Applier) mysqlGTIDMode() error {
	query := `SELECT @@GTID_MODE`
	var gtidMode string
	if err := a.singletonDB.QueryRow(query).Scan(&gtidMode); err != nil {
		return err
	}
	if gtidMode != "ON" {
		return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
	}
	return nil
}

func (a *Applier) mysqlServerUUID() string {
	query := `SELECT @@SERVER_UUID`
	var server_uuid string
	if err := a.singletonDB.QueryRow(query).Scan(&server_uuid); err != nil {
		return ""
	}
	return server_uuid
}

func (a *Applier) Shutdown() error {
	if err := a.stanSub.Unsubscribe(); err != nil {
		return err
	}
	if err := a.stanConn.Close(); err != nil {
		return err
	}

	if err := usql.CloseDBs(a.dbs...); err != nil {
		return err
	}

	a.logger.Printf("[INFO] mysql.applier: closed applier connection.")

	return nil
}
