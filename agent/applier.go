package agent

import (
	gosql "database/sql"
	"fmt"
	"net"
	"strconv"
	"time"

	gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
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
	eventChans  []chan *usql.StreamEvent

	natsConn *nats.Conn
	gnatsd   *gnatsd.Server
}

func NewApplier(cfg *uconf.Config) *Applier {
	return &Applier{
		cfg:        cfg,
		eventChans: newEventChans(cfg.WorkerCount),
	}
}

func newEventChans(count int) []chan *usql.StreamEvent {
	events := make([]chan *usql.StreamEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, make(chan *usql.StreamEvent, 1000))
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

	if err := a.initiateStreaming(); err != nil {
		return err
	}

	for i := 0; i < a.cfg.WorkerCount; i++ {
		go a.applyEventQuery(a.dbs[i], a.eventChans[i])
	}

	return nil
}

func (a *Applier) applyEventQuery(db *gosql.DB, eventChan chan *usql.StreamEvent) {
	idx := 0
	count := a.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()

	var err error
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				return
			}
			idx++
			if event.Tp == usql.Ddl {
				err = usql.ExecuteSQL(db, sqls, args, true)
				if err != nil {
					a.cfg.PanicAbort <- err
				}

				err = usql.ExecuteSQL(db, []string{event.Sql}, [][]interface{}{event.Args}, false)
				if err != nil {
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
				err = usql.ExecuteSQL(db, sqls, args, true)
				if err != nil {
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
				err = usql.ExecuteSQL(db, sqls, args, true)
				if err != nil {
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

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) initiateStreaming() error {
	nc, err := nats.Connect(fmt.Sprintf("nats://%s", a.cfg.NatsAddr))
	if err != nil {
		return err
	}

	c, _ := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	a.natsConn = nc

	if _, err := c.Subscribe("subject", func(event *usql.StreamEvent) {
		idx := int(usql.GenHashKey(event.Key)) % a.cfg.WorkerCount
		a.eventChans[idx] <- event
	}); err != nil {
		return err
	}

	if err := c.LastError(); err != nil {
		return err
	}
	return nil
}

func (a *Applier) setupNatsServer() error {
	host, port, err := net.SplitHostPort(a.cfg.NatsAddr)
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	opts := gnatsd.Options{
		Host: host,
		Port: p,
		// MAX_PAYLOAD_SIZE is the maximum allowed payload size. Should be using
		// something different if > 1MB payloads are needed.
		MaxPayload: (5 * 1024 * 1024),
		// DEFAULT_MAX_CONNECTIONS is the default maximum connections allowed.
		MaxConn: (64 * 1024),
		Trace:   true,
		Debug:   true,
	}
	gnats := gnatsd.New(&opts)
	go gnats.Start()
	a.gnatsd = gnats
	return nil
}

func (a *Applier) initDBConnections() (err error) {
	if a.singletonDB, _, err = sqlutils.GetDB(a.cfg.Apply.ConnCfg.GetDBUri()); err != nil {
		return err
	}
	a.singletonDB.SetMaxOpenConns(1)

	if a.dbs, err = GetDBs(a.cfg.Apply.ConnCfg, a.cfg.WorkerCount+1); err != nil {
		return err
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

func closeEventChans(events []chan *usql.StreamEvent) {
	for _, ch := range events {
		close(ch)
	}
}

func (a *Applier) Shutdown() error {
	usql.CloseDBs(a.dbs...)

	closeEventChans(a.eventChans)

	a.natsConn.Close()
	a.gnatsd.Shutdown()
	return nil
}
