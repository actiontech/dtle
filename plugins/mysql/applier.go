package mysql

import (
	"bytes"
	gosql "database/sql"
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
	"time"

	gnatsd "github.com/nats-io/gnatsd/server"
	stan "github.com/nats-io/go-nats-streaming"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/ngaut/log"

	uconf "udup/config"
	ubinlog "udup/plugins/mysql/binlog"
	usql "udup/plugins/mysql/sql"
)

type Applier struct {
	cfg         *uconf.DriverConfig
	dbs         []*gosql.DB
	singletonDB *gosql.DB
	eventChans  []chan usql.StreamEvent

	eventsChannel chan *ubinlog.BinlogEvent
	currentTx     *ubinlog.Transaction_t
	txChan        chan *ubinlog.Transaction_t

	stanConn stan.Conn
	stanSub  stan.Subscription
	stand    *stand.StanServer
	gnatsd   *gnatsd.Server
}

func NewApplier(cfg *uconf.DriverConfig) *Applier {
	return &Applier{
		cfg:        cfg,
		eventChans: newEventChans(cfg.WorkerCount),
		txChan:     make(chan *ubinlog.Transaction_t, 100),
	}
}

func newEventChans(count int) []chan usql.StreamEvent {
	events := make([]chan usql.StreamEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, make(chan usql.StreamEvent, 1000))
	}

	return events
}

func (a *Applier) InitiateApplier() error {
	log.Infof("Apply binlog events onto the datasource :%v", a.cfg.ConnCfg.String())
	if err := a.setupNatsServer(); err != nil {
		return err
	}

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

func (a *Applier) applyTx(db *gosql.DB, transaction *ubinlog.Transaction_t) error {
	if len(transaction.Query) == 0 {
		return nil
	}

	var lastFde, lastSID string
	var lastGNO int64
	if lastFde != transaction.Fde {
		lastFde = transaction.Fde // IMO it would comare the internal pointer first
		_, err := db.Exec(lastFde)
		if err != nil {
			return err
		}
	}

	if lastSID != transaction.SID || lastGNO != transaction.GNO {
		lastSID = transaction.SID
		lastGNO = transaction.GNO
		_, err := db.Exec(fmt.Sprintf(`SET GTID_NEXT='%s:%d'`, lastSID, lastGNO))
		if err != nil {
			return err
		}

		txn, err := db.Begin()
		if err != nil {
			return err
		}

		err = txn.Commit()
		if err != nil {
			return err
		}

		if _, err := db.Exec(`SET GTID_NEXT='AUTOMATIC'`); err != nil {
			return err
		}
		a.cfg.GtidCh <- fmt.Sprintf("%s:1-%d", lastSID, lastGNO)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, query := range transaction.Query {
		if query == "" {
			return nil
		}

		_, err = tx.Exec(query)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (a *Applier) startApplierWorker(i int, db *gosql.DB) {
	for tx := range a.txChan {
		if tx != nil {
			err := a.applyTx(db, tx)
			if err != nil {
				a.cfg.ErrCh <- fmt.Errorf("Error applying tx: %v", err)
			}
		}
	}
}

func (a *Applier) initNatSubClient() (err error) {
	sc, err := stan.Connect("test-cluster", "sub1", stan.NatsURL(fmt.Sprintf("nats://%s", a.cfg.NatsAddr)))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, fmt.Sprintf("nats://%s", a.cfg.NatsAddr))
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
	sub, err := a.stanConn.Subscribe("subject", func(m *stan.Msg) {
		tx := &ubinlog.Transaction_t{}
		if err := Decode(m.Data, tx); err != nil {
			a.cfg.ErrCh <- fmt.Errorf("Subscribe err:%v", err)
		}
		/*idx := int(usql.GenHashKey(event.Key)) % a.cfg.WorkerCount
		a.eventChans[idx] <- event*/
		a.txChan <- tx
	})

	if err != nil {
		return fmt.Errorf("Unexpected error on Subscribe, got %v", err)
	}
	a.stanSub = sub
	return nil
}

func (a *Applier) setupNatsServer() error {
	host, port, err := net.SplitHostPort(a.cfg.NatsAddr)
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	nOpts := gnatsd.Options{
		Host:       host,
		Port:       p,
		MaxPayload: (100 * 1024 * 1024),
		Trace:      true,
		Debug:      true,
	}
	gnats := gnatsd.New(&nOpts)
	go gnats.Start()
	// Wait for accept loop(s) to be started
	if !gnats.ReadyForConnections(10 * time.Second) {
		log.Infof("Unable to start NATS Server in Go Routine")
	}
	a.gnatsd = gnats
	sOpts := stand.GetDefaultOptions()
	if a.cfg.StoreType == "FILE" {
		sOpts.StoreType = a.cfg.StoreType
		sOpts.FilestoreDir = a.cfg.FilestoreDir
	}
	sOpts.NATSServerURL = fmt.Sprintf("nats://%s", a.cfg.NatsAddr)
	s := stand.RunServerWithOpts(sOpts, nil)
	a.stand = s
	return nil
}

func (a *Applier) initDBConnections() (err error) {
	if a.singletonDB, err = usql.CreateDB(a.cfg.ConnCfg); err != nil {
		return err
	}
	a.singletonDB.SetMaxOpenConns(1)
	if err := a.mysqlGTIDMode(); err != nil {
		return err
	}

	if a.dbs, err = usql.CreateDBs(a.cfg.ConnCfg, a.cfg.WorkerCount+1); err != nil {
		return err
	}
	return nil
}

func (a *Applier) mysqlGTIDMode() error {
	query := `SELECT @@gtid_mode`
	var gtidMode string
	if err := a.singletonDB.QueryRow(query).Scan(&gtidMode); err != nil {
		return err
	}
	if gtidMode != "ON" {
		return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
	}
	return nil
}

func (a *Applier) stopFlag() bool {
	return a.cfg.Enabled
}

func closeEventChans(events []chan usql.StreamEvent) {
	for _, ch := range events {
		close(ch)
	}
}

func (a *Applier) Shutdown() error {
	if !a.stopFlag() {
		return nil
	}
	if err :=a.stanSub.Unsubscribe(); err != nil {
		return err
	}
	if err :=a.stanConn.Close(); err != nil {
		return err
	}
	a.stand.Shutdown()
	a.gnatsd.Shutdown()
	closeEventChans(a.eventChans)

	if err :=usql.CloseDBs(a.dbs...); err != nil {
		return err
	}
	a.cfg.Enabled = false
	log.Infof("Closed applier connection.")
	return nil
}
