package agent

import (
	gosql "database/sql"
	"fmt"
	"net"
	"strconv"

	gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/ngaut/log"
	"github.com/outbrain/golib/sqlutils"

	usql "udup/agent/mysql"
	uconf "udup/config"
)

type Applier struct {
	cfg           *uconf.Config
	db            *gosql.DB
	eventsChannel chan *usql.StreamEvent

	natsConn *nats.Conn
	gnatsd   *gnatsd.Server
}

func NewApplier(cfg *uconf.Config) *Applier {
	return &Applier{
		cfg:           cfg,
		eventsChannel: make(chan *usql.StreamEvent, EventsChannelBufferSize),
	}
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

	go a.applyEventQuery()

	return nil
}

func (a *Applier) applyEventQuery() (err error) {
	for event := range a.eventsChannel {
		err = func() error {
			tx, err := a.db.Begin()
			if err != nil {
				a.cfg.PanicAbort <- err
			}
			sessionQuery := `SET
			SESSION time_zone = '+00:00',
			sql_mode = CONCAT(@@session.sql_mode, ',STRICT_ALL_TABLES')
			`
			if _, err := tx.Exec(sessionQuery); err != nil {
				a.cfg.PanicAbort <- err
			}
			if _, err := tx.Exec(event.Sql, event.Args...); err != nil {
				a.cfg.PanicAbort <- err
			}
			if err := tx.Commit(); err != nil {
				a.cfg.PanicAbort <- err
			}
			return nil
		}()

		if err != nil {
			err = fmt.Errorf("%s; query=%s; args=%+v", err.Error(), event.Sql, event.Args)
			a.cfg.PanicAbort <- err
		}
	}
	return nil
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
		a.eventsChannel <- event
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
		// MAX_PENDING_SIZE is the maximum outbound size (in bytes) per client.
		MaxPending: (20 * 1024 * 1024),
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
	if a.db, _, err = sqlutils.GetDB(a.cfg.Apply.ConnCfg.GetDBUri()); err != nil {
		return err
	}

	if err := a.validateConnection(); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (a *Applier) validateConnection() error {
	query := `select @@global.port`
	var port int
	if err := a.db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != a.cfg.Apply.ConnCfg.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", a.cfg.Apply.ConnCfg)
	return nil
}

func (a *Applier) Shutdown() error {
	usql.CloseDBs(a.db)

	close(a.eventsChannel)

	a.natsConn.Close()
	a.gnatsd.Shutdown()
	return nil
}
