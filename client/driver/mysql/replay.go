package mysql

import (
	gosql "database/sql"
	"fmt"
	"log"

	//"github.com/nats-io/go-nats/encoders/protobuf"
	"github.com/nats-io/go-nats"
	"github.com/outbrain/golib/sqlutils"

	"udup/client/driver/mysql/base"
)

type Replayer struct {
	db           *gosql.DB
	singletonDB  *gosql.DB
	mysqlContext *base.MySQLContext
	logger       *log.Logger
	panicAbort   chan error
}

func NewReplayer(logger *log.Logger, mysqlContext *base.MySQLContext) *Replayer {
	return &Replayer{
		mysqlContext: mysqlContext,
		logger:       logger,
		panicAbort:   make(chan error),
	}
}

func InitiateReplayer(logger *log.Logger, mysqlContext *base.MySQLContext, c *nats.EncodedConn) (*Replayer, error) {
	r := NewReplayer(logger, mysqlContext)
	if err := r.InitDBConnections(); err != nil {
		return nil, err
	}

	if err := r.replayEvent(c); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Replayer) InitDBConnections() (err error) {
	replayerUri := r.mysqlContext.ConnectionConfig.GetDBUri()
	if r.db, _, err = sqlutils.GetDB(replayerUri); err != nil {
		return err
	}
	singletonReplayerUri := fmt.Sprintf("%s?timeout=0", replayerUri)
	if r.singletonDB, _, err = sqlutils.GetDB(singletonReplayerUri); err != nil {
		return err
	}
	r.singletonDB.SetMaxOpenConns(1)
	if err := r.validateConnection(r.db); err != nil {
		return err
	}
	if err := r.validateConnection(r.singletonDB); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (r *Replayer) validateConnection(db *gosql.DB) error {
	query := `select @@global.port`
	var port int
	if err := db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != r.mysqlContext.ConnectionConfig.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	r.logger.Printf("[INFO] client: replayer connection validated on %+v:%v", r.mysqlContext.ConnectionConfig.Host, r.mysqlContext.ConnectionConfig.Port)
	return nil
}

func (r *Replayer) replayEvent(c *nats.EncodedConn) error {
	if _, err := c.Subscribe("test", func(entry *EventsStream) {
		//eventsChannel <- entry
		tx, err := r.db.Begin()
		if err != nil {
			r.logger.Printf("[ERR] Begin:%v", err)
			r.panicAbort <- err
		}
		if _, err := tx.Exec(`SET
			SESSION time_zone = '+00:00',
			sql_mode = CONCAT(@@session.sql_mode, ',STRICT_ALL_TABLES')
			`); err != nil {
			r.logger.Printf("[ERR] SET:%v", err)
			r.panicAbort <- err
		}
		if _, err := tx.Exec(entry.Sql, entry.Args...); err != nil {
			r.panicAbort <- err
			r.logger.Printf("[ERR] Exec:%v", err)
		}
		if err := tx.Commit(); err != nil {
			r.logger.Printf("[ERR] Commit:%v", err)
			r.panicAbort <- err
		}
	}); err != nil {
		return err
	}

	c.Flush()

	if err := c.LastError(); err != nil {
		r.logger.Printf("%v", err)
		return err
	}

	for {
		select {
		case err := <-r.panicAbort:
			return err
		default:
			//
		}
	}

	return nil
}
