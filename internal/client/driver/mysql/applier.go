package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
	//"math"
	"bytes"
	"encoding/gob"
	//"encoding/base64"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"udup/internal/client/driver/mysql/base"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"
	"udup/utils"
	umconf "udup/internal/config/mysql"
	"context"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

type applierTableItem struct {
	columns *umconf.ColumnList
	psInsert *gosql.Stmt
	psDelete *gosql.Stmt
	psUpdate *gosql.Stmt
}
func (ait *applierTableItem) Reset() {
	// TODO handle err of `.Close()`?
	if ait.psInsert != nil {
		ait.psInsert.Close()
		ait.psInsert = nil
	}
	if ait.psDelete != nil {
		ait.psDelete.Close()
		ait.psDelete = nil
	}
	if ait.psUpdate != nil {
		ait.psUpdate.Close()
		ait.psUpdate = nil
	}
	ait.columns = nil
}

type mapSchemaTableItems map[string](map[string](*applierTableItem))

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.
type Applier struct {
	logger             *log.Entry
	subject            string
	tp                 string
	mysqlContext       *config.MySQLDriverConfig
	dbs                []*sql.Conn
	db                 *gosql.DB
	retrievedGtidSet   string
	executedIntervals  gomysql.IntervalSlice
	currentCoordinates *models.CurrentCoordinates
	tableItems         mapSchemaTableItems

	rowCopyComplete     chan bool
	rowCopyCompleteFlag int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue            chan *dumpEntry
	applyDataEntryQueue      chan *binlog.BinlogEntry
	applyGroupDataEntryQueue chan []*binlog.BinlogEntry
	applyBinlogTxQueue       chan *binlog.BinlogTx
	applyBinlogGroupTxQueue  chan []*binlog.BinlogTx
	lastAppliedBinlogTx      *binlog.BinlogTx

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult
	wg       sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewApplier(subject, tp string, cfg *config.MySQLDriverConfig, logger *log.Logger) *Applier {
	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	a := &Applier{
		logger:                   entry,
		subject:                  subject,
		tp:                       tp,
		mysqlContext:             cfg,
		currentCoordinates:       &models.CurrentCoordinates{},
		tableItems:               make(mapSchemaTableItems),
		rowCopyComplete:          make(chan bool, 1),
		copyRowsQueue:            make(chan *dumpEntry, 24),
		applyDataEntryQueue:      make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize * 2),
		applyGroupDataEntryQueue: make(chan []*binlog.BinlogEntry, cfg.ReplChanBufferSize * 2),
		applyBinlogTxQueue:       make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize * 2),
		applyBinlogGroupTxQueue:  make(chan []*binlog.BinlogTx, cfg.ReplChanBufferSize * 2),
		waitCh:                   make(chan *models.WaitResult, 1),
		shutdownCh:               make(chan struct{}),
	}
	return a
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (a *Applier) sleepWhileTrue(operation func() (bool, error)) error {
	for {
		shouldSleep, err := operation()
		if err != nil {
			return err
		}
		if !shouldSleep {
			return nil
		}
		time.Sleep(time.Second)
	}
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (a *Applier) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(a.mysqlContext.MaxRetries)
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			// sleep after previous iteration
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
		// there's an error. Let's try again.
	}
	if len(notFatalHint) == 0 {
		return err
	}
	return err
}

// Run executes the complete apply logic.
func (a *Applier) Run() {
	a.logger.Printf("mysql.applier: Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	a.mysqlContext.StartTime = time.Now()
	if err := a.initDBConnections(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}
	if err := a.initNatSubClient(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	if err := a.initiateStreaming(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	go a.executeWriteFuncs()

	if a.tp == models.JobTypeMig {
		var completeFlag string
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr_complete", a.subject), func(m *gonats.Msg) {
			completeFlag = string(m.Data)
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			a.onError(TaskStateDead, err)
		}

		for {
			if completeFlag != "" && a.rowCopyCompleteFlag == 1 {
				switch completeFlag {
				case "0":
					a.onError(TaskStateComplete, nil)
					break
				default:
					binlogCoordinates, err := base.GetSelfBinlogCoordinates(a.db)
					if err != nil {
						a.onError(TaskStateDead, err)
						break
					}
					if a.mysqlContext.Gtid != "" && binlogCoordinates.GtidSet != "" {
						equals, err := base.ContrastGtidSet(a.mysqlContext.Gtid, binlogCoordinates.GtidSet)
						if err != nil {
							a.onError(TaskStateDead, err)
							break
						}
						if equals {
							a.onError(TaskStateComplete, nil)
							break
						}
					}
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (a *Applier) readCurrentBinlogCoordinates() error {
	query := `show master status`
	foundMasterStatus := false
	err := sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		if m.GetString("Executed_Gtid_Set") != "" {
			gtidSet, err := gomysql.ParseMysqlGTIDSet(m.GetString("Executed_Gtid_Set"))
			if err != nil {
				return err
			}

			a.mysqlContext.Gtid = gtidSet.String()
		}
		foundMasterStatus = true

		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
	}

	return nil
}

func (a *Applier) onApplyTxStructWithSuper(dbApplier *sql.Conn, binlogTx *binlog.BinlogTx) error {
	dbApplier.DbMutex.Lock()
	defer func() {
		_, err := sql.ExecNoPrepare(dbApplier.Db, `commit;set gtid_next='automatic'`)
		if err != nil {
			a.onError(TaskStateDead, err)
		}
		dbApplier.DbMutex.Unlock()
	}()

	if binlogTx.Fde != "" && dbApplier.Fde != binlogTx.Fde {
		dbApplier.Fde = binlogTx.Fde // IMO it would comare the internal pointer first
		_, err := sql.ExecNoPrepare(dbApplier.Db, binlogTx.Fde)
		if err != nil {
			return err
		}
	}

	_, err := sql.ExecNoPrepare(dbApplier.Db, fmt.Sprintf(`set gtid_next='%s:%d'`, binlogTx.SID, binlogTx.GNO))
	if err != nil {
		return err
	}
	var ignoreError error
	if binlogTx.Query == "" {
		_, err = sql.ExecNoPrepare(dbApplier.Db, `begin;commit`)
		if err != nil {
			return err
		}
	} else {
		_, err := sql.ExecNoPrepare(dbApplier.Db, binlogTx.Query)
		if err != nil {
			if !sql.IgnoreError(err) {
				//SELECT FROM_BASE64('')
				a.logger.Errorf("mysql.applier: exec gtid:[%s:%d] error: %v", binlogTx.SID, binlogTx.GNO, err)
				return err
			}
			a.logger.Warnf("mysql.applier: exec gtid:[%s:%d],ignore error: %v", binlogTx.SID, binlogTx.GNO, err)
			ignoreError = err
		}
	}

	if ignoreError != nil {
		_, err := sql.ExecNoPrepare(dbApplier.Db, fmt.Sprintf(`commit;set gtid_next='%s:%d'`, binlogTx.SID, binlogTx.GNO))
		if err != nil {
			return err
		}
		_, err = sql.ExecNoPrepare(dbApplier.Db, `begin;commit`)
		if err != nil {
			return err
		}
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (a *Applier) executeWriteFuncs() {
	go func() {
		var stopLoop = false
		for !stopLoop {
			select {
			case copyRows := <-a.copyRowsQueue:
				if nil != copyRows {
					if copyRows.DbSQL != "" || len(copyRows.TbSQL) > 0 {
						if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
							a.onError(TaskStateDead, err)
						}
					} else {
						if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
							a.onError(TaskStateDead, err)
						}
					}
				}
			case <-a.rowCopyComplete:
				stopLoop = true
			case <-a.shutdownCh:
				stopLoop = true
			case <-time.After(10 * time.Second):
				a.logger.Debugf("mysql.applier: no copyRows for 10s.")
			}
		}
	}()

	if a.mysqlContext.Gtid == "" {
		a.logger.Printf("mysql.applier: Operating until row copy is complete")
		a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
		for {
			if atomic.LoadInt64(&a.rowCopyCompleteFlag) == 1 && a.mysqlContext.TotalRowsCopied == a.mysqlContext.TotalRowsReplay {
				a.rowCopyComplete <- true
				a.logger.Printf("mysql.applier: Rows copy complete.number of rows:%d", a.mysqlContext.TotalRowsReplay)
				a.mysqlContext.Gtid = a.currentCoordinates.RetrievedGtidSet
				break
			}
			if a.shutdown {
				break
			}
			time.Sleep(time.Second)
		}
	}

	var dbApplier *sql.Conn
OUTER:
	for {
		select {
		case groupEntry := <-a.applyGroupDataEntryQueue:
			// this chan is used for single worker
			{
				if len(groupEntry) == 0 {
					continue
				}
				/*if a.mysqlContext.MySQLServerUuid == binlogEntry.Coordinates.OSID {
					continue
				}*/

				for idx, binlogEntry := range groupEntry {
					dbApplier = a.dbs[idx%a.mysqlContext.ParallelWorkers]
					//go func(entry *binlog.BinlogEntry) {
					//a.wg.Add(1)
					if err := a.ApplyBinlogEvent(a.dbs[0], binlogEntry); err != nil {
						a.onError(TaskStateDead, err)
					}
					//a.wg.Done()
					//}(binlogEntry)
				}
				//a.wg.Wait() // Waiting for all goroutines to finish

				//a.logger.Debugf("mysql.applier: apply binlogEntry: %+v", groupEntry[len(groupEntry)-1].Coordinates.GNO)

				if !a.shutdown {
					a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", groupEntry[len(groupEntry)-1].Coordinates.SID, groupEntry[len(groupEntry)-1].Coordinates.GNO)
				}
			}
		case groupTx := <-a.applyBinlogGroupTxQueue:
			// this chan is used for parallel workers
			{
				if len(groupTx) == 0 {
					continue
				}
				for idx, binlogTx := range groupTx {
					dbApplier = a.dbs[idx%a.mysqlContext.ParallelWorkers]
					go func(tx *binlog.BinlogTx) {
						a.wg.Add(1)
						if err := a.onApplyTxStructWithSuper(dbApplier, tx); err != nil {
							a.onError(TaskStateDead, err)
						}
						a.wg.Done()
					}(binlogTx)
				}
				a.wg.Wait() // Waiting for all goroutines to finish

				if !a.shutdown {
					a.lastAppliedBinlogTx = groupTx[len(groupTx)-1]
					a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", a.lastAppliedBinlogTx.SID, a.lastAppliedBinlogTx.GNO)
				}
			}
		case <-a.shutdownCh:
			break OUTER
		default:
			time.Sleep(time.Second)
		}
	}
}

func (a *Applier) initNatSubClient() (err error) {
	natsAddr := fmt.Sprintf("nats://%s", a.mysqlContext.NatsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		a.logger.Errorf("mysql.applier: Can't connect nats server %v. make sure a nats streaming server is running.%v", natsAddr, err)
		return err
	}
	a.logger.Debugf("mysql.applier: Connect nats server %v", natsAddr)
	a.natsConn = sc
	return nil
}

// Decode
func Decode(data []byte, vPtr interface{}) (err error) {
	msg, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	return gob.NewDecoder(bytes.NewBuffer(msg)).Decode(vPtr)
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) initiateStreaming() error {
	if a.mysqlContext.Gtid == "" {
		a.mysqlContext.MarkRowCopyStartTime()
		a.logger.Debugf("mysql.applier: nats subscribe")
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
			a.logger.Debugf("mysql.applier: recv a msg")
			dumpData := &dumpEntry{}
			if err := Decode(m.Data, dumpData); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.copyRowsQueue <- dumpData
			a.logger.Debugf("mysql.applier: copyRowsQueue: %v", len(a.copyRowsQueue))
			a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.logger.Debugf("mysql.applier: after publish nats reply")
			atomic.AddInt64(&a.mysqlContext.RowsEstimate, dumpData.TotalCount)
		})
		/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
			return err
		}*/

		_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
			dumpData := &dumpStatResult{}
			if err := Decode(m.Data, dumpData); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.currentCoordinates.RetrievedGtidSet = dumpData.Gtid
			a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			atomic.AddInt64(&a.mysqlContext.TotalRowsCopied, dumpData.TotalCount)
			atomic.StoreInt64(&a.rowCopyCompleteFlag, 1)
		})
		if err != nil {
			return err
		}
	}

	if a.mysqlContext.ApproveHeterogeneous {
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
			var binlogEntries binlog.BinlogEntries
			if err := Decode(m.Data, &binlogEntries); err != nil {
				a.onError(TaskStateDead, err)
			}

			a.logger.Debugf("applier. incr. recv. nEntries: %v", len(binlogEntries.Entries))
			for _, binlogEntry := range binlogEntries.Entries {
				a.applyDataEntryQueue <- binlogEntry
				a.currentCoordinates.RetrievedGtidSet = binlogEntry.Coordinates.GetGtidForThisTx()
				atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)
			}
			a.mysqlContext.Stage = models.StageWaitingForMasterToSendEvent

			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			return err
		}

		go func() {
			var lastCommitted int64
			//timeout := time.After(100 * time.Millisecond)
			groupEntry := []*binlog.BinlogEntry{}
		OUTER:
			for {
				select {
				case binlogEntry := <-a.applyDataEntryQueue:
					if nil == binlogEntry {
						continue
					}
					/*if a.mysqlContext.MySQLServerUuid == binlogTx.SID {
						continue
					}*/
					if a.mysqlContext.ParallelWorkers <= 1 {
						if err := a.ApplyBinlogEvent(a.dbs[0], binlogEntry); err != nil {
							a.onError(TaskStateDead, err)
							break OUTER
						}
					} else {
						if binlogEntry.Coordinates.LastCommitted == lastCommitted {
							groupEntry = append(groupEntry, binlogEntry)
						} else {
							if len(groupEntry) != 0 {
								a.applyGroupDataEntryQueue <- groupEntry
								groupEntry = []*binlog.BinlogEntry{}
							}
							groupEntry = append(groupEntry, binlogEntry)
						}
						lastCommitted = binlogEntry.Coordinates.LastCommitted
					}
				case <-time.After(100 * time.Millisecond):
					if len(groupEntry) != 0 {
						a.applyGroupDataEntryQueue <- groupEntry
						groupEntry = []*binlog.BinlogEntry{}
					}
				case <-a.shutdownCh:
					break OUTER
				}
			}
		}()
	} else {
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr", a.subject), func(m *gonats.Msg) {
			var binlogTx []*binlog.BinlogTx
			if err := Decode(m.Data, &binlogTx); err != nil {
				a.onError(TaskStateDead, err)
			}
			for _, tx := range binlogTx {
				a.applyBinlogTxQueue <- tx
			}
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			return err
		}
		/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
			return err
		}*/
	}

	go func() {
		var lastCommitted int64
		var err error
		//timeout := time.After(100 * time.Millisecond)
		groupTx := []*binlog.BinlogTx{}
	OUTER:
		for {
			select {
			case binlogTx := <-a.applyBinlogTxQueue:
				if nil == binlogTx {
					continue
				}
				if a.mysqlContext.MySQLServerUuid == binlogTx.SID {
					continue
				}
				if a.mysqlContext.ParallelWorkers <= 1 {
					if err = a.onApplyTxStructWithSuper(a.dbs[0], binlogTx); err != nil {
						a.onError(TaskStateDead, err)
						break OUTER
					}

					if !a.shutdown {
						a.lastAppliedBinlogTx = binlogTx
						a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", a.lastAppliedBinlogTx.SID, a.lastAppliedBinlogTx.GNO)
					}
				} else {
					if binlogTx.LastCommitted == lastCommitted {
						groupTx = append(groupTx, binlogTx)
					} else {
						if len(groupTx) != 0 {
							a.applyBinlogGroupTxQueue <- groupTx
							groupTx = []*binlog.BinlogTx{}
						}
						groupTx = append(groupTx, binlogTx)
					}
					lastCommitted = binlogTx.LastCommitted
				}
			case <-time.After(100 * time.Millisecond):
				if len(groupTx) != 0 {
					a.applyBinlogGroupTxQueue <- groupTx
					groupTx = []*binlog.BinlogTx{}
				}
			case <-a.shutdownCh:
				break OUTER
			}
		}
	}()

	return nil
}

func (a *Applier) initDBConnections() (err error) {
	applierUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.db, err = sql.CreateDB(applierUri); err != nil {
		return err
	}
	a.db.SetMaxOpenConns(10 + a.mysqlContext.ParallelWorkers)

	if a.dbs, err = sql.CreateConns(a.db, a.mysqlContext.ParallelWorkers); err != nil {
		return err
	}

	if err := a.validateConnection(a.db); err != nil {
		return err
	}
	if err := a.validateServerUUID(); err != nil {
		return err
	}
	/*if err := a.validateTableForeignKeys(); err != nil {
		return err
	}*/
	if err := a.validateGrants(); err != nil {
		a.logger.Errorf("mysql.applier: Unexpected error on validateGrants, got %v", err)
		return err
	}
	if err := a.validateAndReadTimeZone(); err != nil {
		return err
	}
	if a.mysqlContext.ApproveHeterogeneous {
		if err := a.createTableGtidExecuted(); err != nil {
			return err
		}

		a.dbs[0].PsDeleteExecutedGtid, err = a.dbs[0].Db.PrepareContext(context.Background(), fmt.Sprintf("delete from actiontech_udup.gtid_executed where job_uuid = '%s' and source_uuid = ?",
			a.subject))
		if err != nil {
			return err
		}
		a.dbs[0].PsInsertExecutedGtid, err = a.dbs[0].Db.PrepareContext(context.Background(), fmt.Sprintf("replace into actiontech_udup.gtid_executed " +
			"(job_uuid,source_uuid,interval_gtid) " +
			"values ('%s', ?, ?)",
			a.subject))
		if err != nil {
			return err
		}
	}
	/*if err := a.readCurrentBinlogCoordinates(); err != nil {
		return err
	}*/
	/*if err := a.readTableColumns(); err != nil {
		return err
	}*/
	a.logger.Printf("mysql.applier: Initiated on %s:%d, version %+v", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port, a.mysqlContext.MySQLVersion)
	return nil
}

func (a *Applier) validateServerUUID() error {
	query := `SELECT @@SERVER_UUID`
	if err := a.db.QueryRow(query).Scan(&a.mysqlContext.MySQLServerUuid); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (a *Applier) validateConnection(db *gosql.DB) error {
	query := `select @@global.version`
	if err := db.QueryRow(query).Scan(&a.mysqlContext.MySQLVersion); err != nil {
		return err
	}
	// Match the version string (from SELECT VERSION()).
	if strings.HasPrefix(a.mysqlContext.MySQLVersion, "5.6") {
		a.mysqlContext.ParallelWorkers = 1
	}
	a.logger.Debugf("mysql.applier: Connection validated on %s:%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	return nil
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (a *Applier) validateGrants() error {
	query := `show grants for current_user()`
	foundAll := false
	foundSuper := false
	foundDBAll := false

	err := sql.QueryRowsMap(a.db, query, func(rowMap sql.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
				foundSuper = true
			}
			if strings.Contains(grant, "GRANT ALL PRIVILEGES ON `actiontech_udup`.`gtid_executed`") {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON`) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	a.mysqlContext.HasSuperPrivilege = foundSuper

	if foundAll {
		a.logger.Printf("mysql.applier: User has ALL privileges")
		return nil
	}
	if foundSuper {
		a.logger.Printf("mysql.applier: User has SUPER privileges")
		return nil
	}
	if foundDBAll {
		a.logger.Printf("User has ALL privileges on *.*")
		return nil
	}
	a.logger.Debugf("mysql.applier: Privileges: super: %t, ALL on *.*: %t", foundSuper, foundAll)
	//return fmt.Errorf("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
	return nil
}

// validateAndReadTimeZone potentially reads server time-zone
func (a *Applier) validateAndReadTimeZone() error {
	query := `select @@global.time_zone`
	if err := a.db.QueryRow(query).Scan(&a.mysqlContext.TimeZone); err != nil {
		return err
	}

	a.logger.Printf("mysql.applier: Will use time_zone='%s' on applier", a.mysqlContext.TimeZone)
	return nil
}

func (a *Applier) createTableGtidExecuted() error {
	if result, err := sql.QueryResultData(a.db, "SHOW TABLES FROM actiontech_udup LIKE 'gtid_executed'"); nil == err && len(result) > 0 {
		return nil
	}
	query := fmt.Sprintf(`
			CREATE DATABASE IF NOT EXISTS actiontech_udup;
		`)
	if _, err := sql.Exec(a.db, query); err != nil {
		return err
	}

	query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS actiontech_udup.gtid_executed (
				job_uuid char(36) NOT NULL COMMENT 'unique identifier of job',
				source_uuid char(36) NOT NULL COMMENT 'uuid of the source where the transaction was originally executed.',
				interval_gtid text NOT NULL COMMENT 'number of interval.'
			);
		`)
	if _, err := sql.Exec(a.db, query); err != nil {
		return err
	}

	return nil
}

// ExpectProcess expects a process to show up in `SHOW PROCESSLIST` that has given characteristics
func (a *Applier) ExpectProcess(sessionId int64, stateHint, infoHint string) error {
	found := false
	query := `
		select id
			from information_schema.processlist
			where
				id != connection_id()
				and ? in (0, id)
				and state like concat('%', ?, '%')
				and info  like concat('%', ?, '%')
	`
	err := sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		found = true
		return nil
	}, sessionId, stateHint, infoHint)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Cannot find process. Hints: %s, %s", stateHint, infoHint)
	}
	return nil
}

func (a *Applier) ShowStatusVariable(variableName string) (result int64, err error) {
	query := fmt.Sprintf(`show global status like '%s'`, variableName)
	if err := a.db.QueryRow(query).Scan(&variableName, &result); err != nil {
		return 0, err
	}
	return result, nil
}

func (a *Applier) getTableItem(schema string, table string) *applierTableItem {
	schemaItem, ok := a.tableItems[schema]
	if !ok {
		schemaItem = make(map[string]*applierTableItem)
		a.tableItems[schema] = schemaItem
	}

	tableItem, ok := schemaItem[table]
	if !ok {
		tableItem = &applierTableItem{}
		schemaItem[table] = tableItem
	}

	return tableItem
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *Applier) buildDMLEventQuery(dmlEvent binlog.DataEvent) (query *gosql.Stmt, args []interface{}, rowsDelta int64, err error) {
	// Large piece of code deleted here. See git annotate.

	tableItem := a.getTableItem(dmlEvent.DatabaseName, dmlEvent.TableName)
	if tableItem.columns == nil {
		a.logger.Debugf("get tableColumns")
		tableItem.columns, err = base.GetTableColumns(a.db, dmlEvent.DatabaseName, dmlEvent.TableName)
		if err != nil {
			return query, args, -1, err
		}
	} else {
		a.logger.Debugf("reuse tableColumns")
	}

	var tableColumns = tableItem.columns

	switch dmlEvent.DML {
	case binlog.DeleteDML:
		{
			query, uniqueKeyArgs, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			if tableItem.psDelete == nil {
				// TODO multi-threaded apply
				tableItem.psDelete, err = a.dbs[0].Db.PrepareContext(context.Background(), query)
				if err != nil {
					return nil, nil, -1, err
				}
			}
			return tableItem.psDelete, uniqueKeyArgs, -1, err
		}
	case binlog.InsertDML:
		{
			// TODO no need to generate query string every time
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			if tableItem.psInsert == nil {
				a.logger.Debugf("new stmt")
				// TODO multi-threaded apply
				tableItem.psInsert, err = a.dbs[0].Db.PrepareContext(context.Background(), query)
				if err != nil {
					return nil, nil, -1, err
				}
			} else {
				a.logger.Debugf("reuse stmt")
			}
			return tableItem.psInsert, sharedArgs, 1, err
		}
	case binlog.UpdateDML:
		{
			query, sharedArgs, uniqueKeyArgs, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues(), dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)
			if tableItem.psUpdate == nil {
				// TODO multi-threaded apply
				tableItem.psUpdate, err = a.dbs[0].Db.PrepareContext(context.Background(), query)
				if err != nil {
					return nil, nil, -1, err
				}
			}

			return tableItem.psUpdate, args, 0, err
		}
	}
	return nil, args, 0, fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)
}

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *Applier) ApplyBinlogEvent(dbApplier *sql.Conn, binlogEntry *binlog.BinlogEntry) error {
	var totalDelta int64
	var err error

	thisInterval := gomysql.Interval{Start: binlogEntry.Coordinates.GNO, Stop: binlogEntry.Coordinates.GNO + 1}
	if len(a.executedIntervals) == 0 {
		// udup crash recovery or never executed
		a.executedIntervals, err = base.SelectGtidExecuted(dbApplier.Db, binlogEntry.Coordinates.SID, a.subject)
		if err != nil {
			return err
		}
	}

	if a.executedIntervals.Contain([]gomysql.Interval{thisInterval}) {
		// entry executed
		return nil
	}

	// TODO normalize may affect oringinal intervals
	newInterval := append(a.executedIntervals, thisInterval).Normalize()

	dbApplier.DbMutex.Lock()
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
		a.executedIntervals = newInterval
		if !a.shutdown {
			a.currentCoordinates.RelayMasterLogFile = binlogEntry.Coordinates.LogFile
			a.currentCoordinates.ReadMasterLogPos = binlogEntry.Coordinates.LogPos
			a.currentCoordinates.ExecutedGtidSet = fmt.Sprintf("%s:%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
			a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
		}
		dbApplier.DbMutex.Unlock()
	}()

	for _, event := range binlogEntry.Events {
		switch event.DML {
		case binlog.NotDML:
			a.logger.Debugf("ApplyBinlogEvent: not dml: %v", event.Query)

			if event.CurrentSchema != "" && event.CurrentSchema != dbApplier.CurrentSchema {
				query := fmt.Sprintf("USE %s", event.CurrentSchema)
				a.logger.Debugf("mysql.applier: query: %v", query)
				_, err := tx.Exec(query)
				if err != nil {
					if !sql.IgnoreError(err) {
						a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
						return err
					} else {
						a.logger.Warnf("mysql.applier: Ignore error: %v", err)
					}
				} else {
					dbApplier.CurrentSchema = event.CurrentSchema
				}
			}

			if event.TableName != "" {
				var schema string
				if event.DatabaseName != "" {
					schema = event.DatabaseName
				} else {
					schema = event.CurrentSchema
				}
				a.getTableItem(schema, event.TableName).Reset()
			} else if event.DatabaseName == "" {
				if schemaItem, ok := a.tableItems[event.DatabaseName]; ok {
					for _, v := range schemaItem {
						v.Reset()
					}
				}
				delete(a.tableItems, event.DatabaseName)
			}

			_, err := tx.Exec(event.Query)
			if err != nil {
				if !sql.IgnoreError(err) {
					a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
					return err
				} else {
					a.logger.Warnf("mysql.applier: Ignore error: %v", err)
				}
			}
			a.logger.Debugf("mysql.applier: Exec [%s]", event.Query)
		default:
			stmt, args, rowDelta, err := a.buildDMLEventQuery(event)
			if err != nil {
				a.logger.Errorf("mysql.applier: Build dml query error: %v", err)
				return err
			}

			a.logger.Debugf("ApplyBinlogEvent. args: %v", args)

			_, err = stmt.Exec(args...)
			if err != nil {
				a.logger.Errorf("mysql.applier: gtid: %s:%d, error: %v", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO, err)
				return err
			}
			totalDelta += rowDelta
		}
	}

	_, err = dbApplier.PsDeleteExecutedGtid.Exec(binlogEntry.Coordinates.SID)
	if err != nil {
		return err
	}

	_, err = dbApplier.PsInsertExecutedGtid.Exec(binlogEntry.Coordinates.SID, base.StringInterval(newInterval))
	if err != nil {
		return err
	}
	
	// no error
	a.mysqlContext.Stage = models.StageWaitingForGtidToBeCommitted
	atomic.AddInt64(&a.mysqlContext.TotalDeltaCopied, 1)
	return nil
}

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *dumpEntry) error {
	queries := []string{}
	queries = append(queries, entry.SystemVariablesStatement, entry.SqlMode, entry.DbSQL)
	queries = append(queries, entry.TbSQL...)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
		atomic.AddInt64(&a.mysqlContext.TotalRowsReplay, entry.RowsCount)
	}()
	sessionQuery := `SET @@session.foreign_key_checks = 0`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}
	execQuery := func(query string) error {
		a.logger.Debugf("mysql.applier: Exec [%s]", utils.StrLim(query, 256))
		_, err := tx.Exec(query)
		if err != nil {
			if !sql.IgnoreError(err) {
				a.logger.Errorf("mysql.applier: Exec [%s] error: %v", utils.StrLim(query, 10), err)
				return err
			}
			if !sql.IgnoreExistsError(err) {
				a.logger.Warnf("mysql.applier: Ignore error: %v", err)
			}
		}
		return nil
	}

	for _, query := range queries {
		if query == "" {
			continue
		}
		err := execQuery(query)
		if err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	BufSizeLimit := 1 * 1024 * 1024 // 1MB. TODO parameterize it
	BufSizeLimitDelta := 1024
	buf.Grow(BufSizeLimit + BufSizeLimitDelta)
	for i, _ := range entry.ValuesX {
		if buf.Len() == 0 {
			buf.WriteString(fmt.Sprintf(`replace into %s.%s values (`, entry.TableSchema, entry.TableName))
		} else {
			buf.WriteString(",(")
		}

		firstCol := true
		for j := range entry.ValuesX[i] {
			if firstCol {
				firstCol = false
			} else {
				buf.WriteByte(',')
			}

			colData := entry.ValuesX[i][j]
			if colData != nil {
				buf.WriteByte('\'')
				buf.WriteString(sql.EscapeValue(string(*colData)))
				buf.WriteByte('\'')
			} else {
				buf.WriteString("NULL")
			}
		}
		buf.WriteByte(')')

		needInsert := (i == len(entry.ValuesX) - 1) || (buf.Len() >= BufSizeLimit)
		// last rows or sql too large

		if needInsert {
			err := execQuery(buf.String())
			buf.Reset()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *Applier) Stats() (*models.TaskStatistics, error) {
	totalRowsReplay := a.mysqlContext.GetTotalRowsReplay()
	rowsEstimate := atomic.LoadInt64(&a.mysqlContext.RowsEstimate)
	totalDeltaCopied := a.mysqlContext.GetTotalDeltaCopied()
	deltaEstimate := atomic.LoadInt64(&a.mysqlContext.DeltaEstimate)

	var progressPct float64
	var backlog, eta string
	if rowsEstimate == 0 && deltaEstimate == 0 {
		progressPct = 0.0
	} else {
		progressPct = 100.0 * float64(totalDeltaCopied+totalRowsReplay) / float64(deltaEstimate+rowsEstimate)
		if a.mysqlContext.Gtid != "" {
			// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
			// and there is no further need to keep updating the value.
			backlog = fmt.Sprintf("%d/%d", len(a.applyDataEntryQueue), cap(a.applyDataEntryQueue))
		} else {
			backlog = fmt.Sprintf("%d/%d", len(a.copyRowsQueue), cap(a.copyRowsQueue))
		}
	}

	var etaSeconds float64 = math.MaxFloat64
	eta = "N/A"
	if progressPct >= 100.0 {
		eta = "0s"
		a.mysqlContext.Stage = models.StageSlaveHasReadAllRelayLog
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := a.mysqlContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsReplay)
		if atomic.LoadInt64(&a.rowCopyCompleteFlag) == 1 {
			totalExpectedSeconds = elapsedRowCopySeconds * float64(deltaEstimate) / float64(totalDeltaCopied)
		}
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "0s"
		}
	}

	taskResUsage := models.TaskStatistics{
		ExecMasterRowCount: totalRowsReplay,
		ExecMasterTxCount:  totalDeltaCopied,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct, 'f', 1, 64),
		ETA:                eta,
		Backlog:            backlog,
		Stage:              a.mysqlContext.Stage,
		CurrentCoordinates: a.currentCoordinates,
		BufferStat: models.BufferStat{
			ApplierTxQueueSize:      len(a.applyBinlogTxQueue),
			ApplierGroupTxQueueSize: len(a.applyBinlogGroupTxQueue),
		},
		Timestamp: time.Now().UTC().UnixNano(),
	}
	if a.natsConn != nil {
		taskResUsage.MsgStat = a.natsConn.Statistics
	}

	return &taskResUsage, nil
}

func (a *Applier) ID() string {
	id := config.DriverCtx{
		DriverConfig: &config.MySQLDriverConfig{
			ReplicateDoDb:     a.mysqlContext.ReplicateDoDb,
			ReplicateIgnoreDb: a.mysqlContext.ReplicateIgnoreDb,
			Gtid:              a.mysqlContext.Gtid,
			NatsAddr:          a.mysqlContext.NatsAddr,
			ParallelWorkers:   a.mysqlContext.ParallelWorkers,
			ConnectionConfig:  a.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		a.logger.Errorf("mysql.applier: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (a *Applier) onError(state int, err error) {
	if a.shutdown {
		return
	}
	switch state {
	case TaskStateComplete:
		a.logger.Printf("mysql.applier: Done migrating")
	case TaskStateRestart:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_restart", a.subject), []byte(a.mysqlContext.Gtid)); err != nil {
				a.logger.Errorf("mysql.applier: Trigger restart extractor : %v", err)
			}
		}
	default:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_error", a.subject), []byte(a.mysqlContext.Gtid)); err != nil {
				a.logger.Errorf("mysql.applier: Trigger extractor shutdown: %v", err)
			}
		}
	}

	a.waitCh <- models.NewWaitResult(state, err)
	a.Shutdown()
}

func (a *Applier) WaitCh() chan *models.WaitResult {
	return a.waitCh
}

func (a *Applier) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}

	if a.natsConn != nil {
		a.natsConn.Close()
	}

	a.shutdown = true
	close(a.shutdownCh)

	if err := sql.CloseDB(a.db); err != nil {
		return err
	}
	if err := sql.CloseConns(a.dbs...); err != nil {
		return err
	}

	//close(a.applyBinlogTxQueue)
	//close(a.applyBinlogGroupTxQueue)
	a.logger.Printf("mysql.applier: Shutting down")
	return nil
}
