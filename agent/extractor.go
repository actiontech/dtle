package agent

import (
	"database/sql"
	"fmt"

	"github.com/ngaut/log"
	"github.com/outbrain/golib/sqlutils"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	uconf "udup/config"
)

type Extractor struct {
	cfg            *uconf.Config
	binlogSyncer   *replication.BinlogSyncer
	binlogStreamer *replication.BinlogStreamer
	db             *sql.DB
}

func NewExtractor(cfg *uconf.Config) *Extractor {
	return &Extractor{
		cfg: cfg,
	}
}

func (e *Extractor) initiateExtractor() error {
	log.Infof("Extract binlog events from the datasource :%v", e.cfg.Extract.ConnCfg)

	if err := e.initDBConnections(); err != nil {
		return err
	}

	go func() {
		log.Debugf("Beginning streaming")
		err := e.streamEvents()
		if err != nil {
			e.cfg.PanicAbort <- err
		}
		log.Debugf("Done streaming")
	}()

	return nil
}

func (e *Extractor) initDBConnections() (err error) {
	if e.db, _, err = sqlutils.GetDB(e.cfg.Extract.ConnCfg.GetDBUri()); err != nil {
		return err
	}
	if err = e.validateConnection(); err != nil {
		return err
	}

	if err = e.initBinlogSyncer(); err != nil {
		return err
	}

	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (e *Extractor) validateConnection() error {
	query := `select @@global.port`
	var port int
	if err := e.db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != e.cfg.Extract.ConnCfg.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	log.Infof("connection validated on %+v", e.cfg.Extract.ConnCfg)
	return nil
}

func (e *Extractor) initBinlogSyncer() (err error) {
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(e.cfg.Extract.ServerID),
		Flavor:          "mysql",
		Host:            e.cfg.Extract.ConnCfg.Host,
		Port:            uint16(e.cfg.Extract.ConnCfg.Port),
		User:            e.cfg.Extract.ConnCfg.User,
		Password:        e.cfg.Extract.ConnCfg.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}
	e.binlogSyncer = replication.NewBinlogSyncer(&cfg)
	return nil
}

func (e *Extractor) streamEvents() (err error) {
	// Start sync with sepcified binlog file and position
	if e.binlogStreamer, err = e.binlogSyncer.StartSync(gomysql.Position{"", uint32(4)}); err != nil {
		return err
	}

	for {
		event, err := e.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			log.Infof("rotate binlog to %v:%v", string(ev.NextLogName), uint32(ev.Position))
		case *replication.RowsEvent:
			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				log.Infof("handle insert event ：%v", event)
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				log.Infof("handle update event ：%v", event)
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				log.Infof("handle delete event ：%v", event)
			}
		case *replication.QueryEvent:
			log.Infof("handle query event ：%v", event)
		case *replication.XIDEvent:
			log.Infof("handle xid event ：%v", event)
		}
	}
	return nil
}

func (e *Extractor) Shutdown() error {
	return nil
}
