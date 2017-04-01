package mysql

import (
	"bytes"
	gosql "database/sql"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"

	uconf "udup/config"
	ulog "udup/logger"
	ubinlog "udup/plugins/mysql/binlog"
	usql "udup/plugins/mysql/sql"
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWait = 5 * time.Second
)

type Applier struct {
	subject     string
	cfg         *uconf.DriverConfig
	dbs         []*gosql.DB
	singletonDB *gosql.DB
	eventChans  []chan usql.StreamEvent

	eventsChannel chan *ubinlog.BinlogEvent
	currentTx     *ubinlog.Transaction_t
	txChan        chan *ubinlog.Transaction_t

	stanConn stan.Conn
	stanSub  stan.Subscription
}

func NewApplier(cfg *uconf.DriverConfig, subject string) *Applier {
	return &Applier{
		subject:    subject,
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
	ulog.Logger.WithFields(logrus.Fields{
		"job":    a.subject,
		"config": a.cfg.ConnCfg.String(),
	}).Info("applier: Apply binlog events")

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
	defer func() {
		_, err := usql.ExecNoPrepare(db, `SET GTID_NEXT='AUTOMATIC'`)
		if err != nil {
			a.cfg.ErrCh <- err
		}
	}()

	for _, query := range transaction.Query {
		if query == "" {
			break
		}

		_, err := usql.ExecNoPrepare(db, query)
		if err != nil {
			if !usql.IgnoreDDLError(err) {
				ulog.Logger.WithField("gtid", fmt.Sprintf("%s:%d", transaction.SID, transaction.GNO)).WithError(err).Error("applier: Exec sql error")
				return err
			} else {
				ulog.Logger.WithField("gtid", fmt.Sprintf("%s:%d", transaction.SID, transaction.GNO)).WithError(err).Error("applier: Ignore ddl error")
			}
		}
	}

	return nil
}

func (a *Applier) startApplierWorker(i int, db *gosql.DB) {
	var lastFde string
	sid := a.mysqlServerUUID()

	for tx := range a.txChan {
		if len(tx.Query) == 0 || sid == tx.SID {
			continue
		}

		if tx.Fde != "" && lastFde != tx.Fde {
			lastFde = tx.Fde // IMO it would comare the internal pointer first
			_, err := usql.ExecNoPrepare(db, lastFde)
			if err != nil {
				a.cfg.ErrCh <- err
				break
			}
		}

		_, err := usql.ExecNoPrepare(db, fmt.Sprintf(`SET GTID_NEXT='%s:%d'`, tx.SID, tx.GNO))
		if err != nil {
			a.cfg.ErrCh <- err
			break
		}

		err = a.applyTx(db, tx)
		if err != nil {
			a.cfg.ErrCh <- err
			break
		}

		a.cfg.GtidCh <- fmt.Sprintf("%s:%d", tx.SID, tx.GNO)
	}
}

func (a *Applier) initNatSubClient() (err error) {
	sc, err := stan.Connect(uconf.DefaultClusterID, uuid.NewV4().String(), stan.NatsURL(fmt.Sprintf("nats://%s", a.cfg.NatsAddr)), stan.ConnectWait(DefaultConnectWait))
	if err != nil {
		ulog.Logger.WithFields(logrus.Fields{
			"err":         err,
			"nats_server": fmt.Sprintf("nats://%s", a.cfg.NatsAddr),
		}).Error("applier: Can't connect nats server.\nMake sure a NATS Streaming Server is running.")
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
			a.cfg.ErrCh <- fmt.Errorf("subscribe err:%v", err)
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
		ulog.Logger.WithFields(logrus.Fields{
			"err": err,
			"job": a.subject,
		}).Error("applier: Select server_uuid error")
		return ""
	}
	return server_uuid
}

func (a *Applier) stopFlag() bool {
	return a.cfg.Running
}

func (a *Applier) Shutdown() error {
	if !a.stopFlag() {
		return nil
	}
	if err := a.stanSub.Unsubscribe(); err != nil {
		return err
	}
	if err := a.stanConn.Close(); err != nil {
		return err
	}

	if err := usql.CloseDBs(a.dbs...); err != nil {
		return err
	}
	a.cfg.Running = false

	ulog.Logger.WithFields(logrus.Fields{
		"job": a.subject,
	}).Info("applier: Closed applier connection.")
	return nil
}
