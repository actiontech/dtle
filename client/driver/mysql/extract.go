package mysql

import (
	gosql "database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"udup/client/driver/mysql/base"
	"udup/client/driver/mysql/binlog"
	"udup/server/structs"

	//"github.com/Shopify/sarama"
	"github.com/nats-io/go-nats"
	//"github.com/nats-io/go-nats/encoders/protobuf"
	"github.com/outbrain/golib/sqlutils"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

type OpType byte

const (
	Insert = iota + 1
	Update
	Del
	Ddl
	Xid
)

type EventsStream struct {
	Tp    OpType
	Sql   string
	Args  []interface{}
	Key   string
	Retry bool
	Pos   gmysql.Position
}

func NewEventsStream(tp OpType, sql string, args []interface{}, key string, retry bool, pos gmysql.Position) *EventsStream {
	return &EventsStream{Tp: tp, Sql: sql, Args: args, Key: key, Retry: retry, Pos: pos}
}

type BinlogEventListener struct {
	async   bool
	onEvent func(event *EventsStream) error
}

const (
	eventsChannelBufferSize       = 1
	ReconnectStreamerSleepSeconds = 5
)

type Extracter struct {
	db             *gosql.DB
	mysqlContext   *base.MySQLContext
	listeners      [](*BinlogEventListener)
	listenersMutex *sync.Mutex
	eventsChannel  chan *EventsStream
	binlogReader   *MySQLReader
	logger         *log.Logger
	panicAbort     chan error
}

func NewExtracter(logger *log.Logger, mysqlContext *base.MySQLContext) *Extracter {
	return &Extracter{
		mysqlContext:   mysqlContext,
		listeners:      [](*BinlogEventListener){},
		listenersMutex: &sync.Mutex{},
		eventsChannel:  make(chan *EventsStream, eventsChannelBufferSize),
		logger:         logger,
		panicAbort:     make(chan error),
	}
}

// InitiateExtracter begins treaming of binary log events and registers listeners for such events
func InitiateExtracter(logger *log.Logger, mysqlContext *base.MySQLContext, c *nats.EncodedConn, tp string) (*Extracter, error) {
	e := NewExtracter(logger, mysqlContext)
	if err := e.InitDBConnections(); err != nil {
		return nil, err
	}

	// validate configs
	if err := e.validateLogSlaveUpdates(); err != nil {
		return nil, err
	}

	e.AddListener(
		false,
		func(entry *EventsStream) error {
			switch tp {
			case structs.JobTypeSync:
				return publishRequest(e, c, entry)
				return nil
			case structs.JobTypeMigrate:
				return nil
			case structs.JobTypeSub:
				return nil
			}
			return nil
		},
	)

	go func() {
		e.logger.Printf("[INFO] client: Beginning streaming")
		err := e.StreamEvents()
		if err != nil {
			e.panicAbort <- err
		}
	}()

	for {
		select {
		case err := <-e.panicAbort:
			return nil, err
		default:
			//
		}
	}
	return e, nil
}

func publishRequest(e *Extracter, c *nats.EncodedConn, entry *EventsStream) error {
	if err := c.Publish("test", entry); err != nil {
		e.panicAbort <- err
		return err
	}
	c.Flush()
	if err := c.LastError(); err != nil {
		e.panicAbort <- err
		return err
	} else {
		e.logger.Printf("Published [%s] : '%v'\n", "test", entry)
		return nil
	}
	return nil
}

func (e *Extracter) InitDBConnections() (err error) {
	extracterUri := e.mysqlContext.ConnectionConfig.GetDBUri()

	if e.db, _, err = sqlutils.GetDB(extracterUri); err != nil {
		return err
	}
	if err := e.validateConnection(); err != nil {
		return err
	}
	if err := e.validateGrants(); err != nil {
		return err
	}
	if err := e.validateBinlogs(); err != nil {
		return err
	}
	if err := e.initBinlogReader(e.mysqlContext.InitialBinlogCoordinates); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (e *Extracter) initBinlogReader(binlogCoordinates *binlog.BinlogCoordinates) error {
	e.logger.Printf("[INFO] client: initBinlogReader :%v", binlogCoordinates)
	mySQLReader, err := NewMySQLReader(e)
	if err != nil {
		return err
	}

	if err := mySQLReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		e.logger.Printf("[ERR] client: ConnectBinlogStreamer err :%v", err)
		return err
	}
	e.binlogReader = mySQLReader
	return nil
}

func (e *Extracter) AddListener(
	async bool, onEvent func(entry *EventsStream) error) (err error) {

	e.listenersMutex.Lock()
	defer e.listenersMutex.Unlock()

	listener := &BinlogEventListener{
		async:   async,
		onEvent: onEvent,
	}
	e.listeners = append(e.listeners, listener)
	return nil
}

func (e *Extracter) notifyListeners(event *EventsStream) {
	e.listenersMutex.Lock()
	defer e.listenersMutex.Unlock()

	for _, listener := range e.listeners {
		if listener.async {
			go func() {
				listener.onEvent(event)
			}()
		} else {
			listener.onEvent(event)
		}
	}
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (e *Extracter) StreamEvents() error {
	go func() {
		for event := range e.eventsChannel {
			if event != nil {
				e.notifyListeners(event)
			}
		}
	}()

	// The next should block and execute forever, unless there's a serious error
	var successiveFailures int64
	var lastAppliedRowsEventHint binlog.BinlogCoordinates
	for {
		if err := e.binlogReader.StreamEvents(e.eventsChannel); err != nil {
			e.logger.Printf("[ERR] client: StreamEvents encountered unexpected error: %+v", err)
			time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

			// See if there's retry overflow
			if e.binlogReader.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
				successiveFailures += 1
			} else {
				successiveFailures = 0
			}
			if successiveFailures > e.mysqlContext.MaxRetries() {
				return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, e.GetReconnectBinlogCoordinates())
			}

			// Reposition at same binlog file.
			lastAppliedRowsEventHint = e.binlogReader.LastAppliedRowsEventHint
			e.logger.Printf("[INFO] client: Reconnecting... Will resume at %+v", lastAppliedRowsEventHint)
			if err := e.initBinlogReader(e.GetReconnectBinlogCoordinates()); err != nil {
				return err
			}
			e.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
		}
	}
}

// validateConnection issues a simple can-connect to MySQL
func (e *Extracter) validateConnection() error {
	query := `select @@global.port`
	var port int
	if err := e.db.QueryRow(query).Scan(&port); err != nil {
		return err
	}
	if port != e.mysqlContext.ConnectionConfig.Port {
		return fmt.Errorf("Unexpected database port reported: %+v", port)
	}
	e.logger.Printf("[INFO] client: streamer connection validated on %+v:%v", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	return nil
}

func (e *Extracter) validateGrants() error {
	query := `show /* udup */ grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	err := sqlutils.QueryRowsMap(e.db, query, func(rowMap sqlutils.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
				foundSuper = true
			}
			if strings.Contains(grant, `REPLICATION CLIENT`) && strings.Contains(grant, ` ON *.*`) {
				foundReplicationClient = true
			}
			if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
				foundReplicationSlave = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", e.mysqlContext.Database[0])) {
				foundDBAll = true
			}
			if StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	e.mysqlContext.HasSuperPrivilege = foundSuper

	if foundAll {
		e.logger.Printf("[INFO] client: User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		e.logger.Printf("[INFO] client: User has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on *.*")
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		e.logger.Printf("[INFO] client: User has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has ALL privileges on *.*")
		return nil
	}
	e.logger.Printf("[DEBUG] client: Privileges: Super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on *.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, foundDBAll)
	return fmt.Errorf("User has insufficient privileges for migration. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on *.*")
}

// StringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func StringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}

// validateBinlogs checks that binary log configuration is good to go
func (e *Extracter) validateBinlogs() error {
	query := `select @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := e.db.QueryRow(query).Scan(&hasBinaryLogs, &e.mysqlContext.OriginalBinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s:%d must have binary logs enabled", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	}
	if e.mysqlContext.RequiresBinlogFormatChange() {
		query := fmt.Sprintf(`show /* udup */ slave hosts`)
		countReplicas := 0
		err := sqlutils.QueryRowsMap(e.db, query, func(rowMap sqlutils.RowMap) error {
			countReplicas++
			return nil
		})
		if err != nil {
			return err
		}
		if countReplicas > 0 {
			return fmt.Errorf("%s:%d has %s binlog_format, but I'm too scared to change it to ROW because it has replicas. Bailing out", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port, e.mysqlContext.OriginalBinlogFormat)
		}
		e.logger.Printf("[INFO] client: %s:%d has %s binlog_format. I will change it to ROW, and will NOT change it back, even in the event of failure.", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port, e.mysqlContext.OriginalBinlogFormat)
	}
	query = `select @@global.binlog_row_image`
	if err := e.db.QueryRow(query).Scan(&e.mysqlContext.OriginalBinlogRowImage); err != nil {
		// Only as of 5.6. We wish to support 5.5 as well
		e.mysqlContext.OriginalBinlogRowImage = "FULL"
	}
	e.mysqlContext.OriginalBinlogRowImage = strings.ToUpper(e.mysqlContext.OriginalBinlogRowImage)

	e.logger.Printf("[INFO] client: binary logs validated on %s:%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	return nil
}

// validateLogSlaveUpdates checks that binary log log_slave_updates is set. i test is not required when migrating on replica or when migrating directly on master
func (e *Extracter) validateLogSlaveUpdates() error {
	query := `select @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := e.db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		return err
	}
	if !logSlaveUpdates {
		return fmt.Errorf("%s:%d must have log_slave_updates enabled", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	}

	e.logger.Printf("[INFO] client: binary logs updates validated on %s:%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	return nil
}

func (e *Extracter) GetCurrentBinlogCoordinates() *binlog.BinlogCoordinates {
	return e.binlogReader.GetCurrentBinlogCoordinates()
}

func (e *Extracter) GetReconnectBinlogCoordinates() *binlog.BinlogCoordinates {
	return &binlog.BinlogCoordinates{LogFile: e.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

func (e *Extracter) Close() (err error) {
	err = e.binlogReader.Close()
	e.logger.Printf("[INFO] client: Closed streamer connection. err=%+v", err)
	return err
}
