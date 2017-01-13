package agent

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
	"github.com/outbrain/golib/sqlutils"

	usql "udup/agent/mysql"
	uconf "udup/config"
)

var (
	waitTime    = 10 * time.Millisecond
	maxWaitTime = 3 * time.Second
)

type Applier struct {
	cfg         *uconf.Config
	dbs         []*gosql.DB
	singletonDB *gosql.DB
	eventChans  []chan usql.StreamEvent

	stanConn stan.Conn
	stanSub  stan.Subscription
	stand    *stand.StanServer
	gnatsd   *gnatsd.Server
}

func NewApplier(cfg *uconf.Config) *Applier {
	return &Applier{
		cfg:        cfg,
		eventChans: newEventChans(cfg.WorkerCount),
	}
}

func newEventChans(count int) []chan usql.StreamEvent {
	events := make([]chan usql.StreamEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, make(chan usql.StreamEvent, 1000))
	}

	return events
}

func (a *Applier) initiateApplier() error {
	log.Infof("Apply binlog events onto the datasource :%v", a.cfg.Apply.ConnCfg)
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
	for i := 0; i < a.cfg.WorkerCount; i++ {
		go a.applyEventQuery(a.dbs[i], a.eventChans[i])
	}

	return nil
}

func (a *Applier) applyEventQuery(db *gosql.DB, eventChan chan usql.StreamEvent) {
	idx := 0
	count := a.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				return
			}
			idx++
			if event.Tp == usql.Gtid {
				if err := usql.ExecuteSQL(db, sqls, args, true); err != nil {
					log.Infof("sql:%v,exec err :%v", sqls, err)
					a.cfg.PanicAbort <- err
				}
				if _, err := sqlutils.ExecNoPrepare(db, event.Sql); err != nil {
					a.cfg.PanicAbort <- err
				}
				txn, err := db.Begin()
				if err != nil {
					a.cfg.PanicAbort <- err
				}

				err = txn.Commit()
				if err != nil {
					a.cfg.PanicAbort <- err
				}
				if _, err := sqlutils.ExecNoPrepare(db, `SET GTID_NEXT='AUTOMATIC'`); err != nil {
					a.cfg.PanicAbort <- err
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()

			} else if event.Tp == usql.Ddl {
				if err := usql.ExecuteSQL(db, sqls, args, true); err != nil {
					log.Infof("sql:%v，ddl err :%v", event.Sql, err)
					a.cfg.PanicAbort <- err
				}
				if err := usql.ExecuteSQL(db, []string{event.Sql}, [][]interface{}{event.Args}, false); err != nil {
					if !usql.IgnoreDDLError(err) {
						a.cfg.PanicAbort <- err
					} else {
						log.Warnf("ignore ddl error :%v", err)
					}
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			} else {
				sqls = append(sqls, event.Sql)
				args = append(args, event.Args)
			}

			if idx >= count {
				if err := usql.ExecuteSQL(db, sqls, args, true); err != nil {
					log.Infof("sql:%v，Begin err :%v", sqls, err)
					a.cfg.PanicAbort <- err
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			}
		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				if err := usql.ExecuteSQL(db, sqls, args, true); err != nil {
					a.cfg.PanicAbort <- err
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = now
			}

			time.Sleep(waitTime)
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
	event := usql.StreamEvent{}
	sub, err := a.stanConn.Subscribe("subject", func(m *stan.Msg) {
		if err := Decode(m.Data, &event); err != nil {
			log.Infof("Subscribe err:%v", err)
			a.cfg.PanicAbort <- err
		}
		idx := int(usql.GenHashKey(event.Key)) % a.cfg.WorkerCount
		a.eventChans[idx] <- event
	})

	if err != nil {
		log.Errorf("Unexpected error on Subscribe, got %v", err)
		return err
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
		Host:  host,
		Port:  p,
		Trace: true,
		Debug: true,
	}
	gnats := gnatsd.New(&nOpts)
	go gnats.Start()
	// Wait for accept loop(s) to be started
	if !gnats.ReadyForConnections(10 * time.Second) {
		return fmt.Errorf("Unable to start NATS Server in Go Routine")
	}
	a.gnatsd = gnats
	sOpts := stand.GetDefaultOptions()
	sOpts.NATSServerURL = fmt.Sprintf("nats://%s", a.cfg.NatsAddr)
	s := stand.RunServerWithOpts(sOpts, nil)
	a.stand = s
	return nil
}

func (a *Applier) initDBConnections() (err error) {
	if a.singletonDB, _, err = sqlutils.GetDB(a.cfg.Apply.ConnCfg.GetDBUri()); err != nil {
		return err
	}
	a.singletonDB.SetMaxOpenConns(1)
	if err := a.mysqlGTIDMode(); err != nil {
		return err
	}

	if a.dbs, err = GetDBs(a.cfg.Apply.ConnCfg, a.cfg.WorkerCount+1); err != nil {
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

func GetDBs(cfg *uconf.ConnectionConfig, count int) ([]*gosql.DB, error) {
	dbs := make([]*gosql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, _, err := sqlutils.GetDB(cfg.GetDBUri())
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeEventChans(events []chan usql.StreamEvent) {
	for _, ch := range events {
		close(ch)
	}
}

func (a *Applier) Shutdown() error {
	usql.CloseDBs(a.dbs...)

	closeEventChans(a.eventChans)

	a.stanSub.Unsubscribe()
	a.stanConn.Close()
	a.stand.Shutdown()
	a.gnatsd.Shutdown()
	return nil
}
