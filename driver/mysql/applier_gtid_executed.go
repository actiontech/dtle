package mysql

import (
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/actiontech/dtle/driver/mysql/base"
	"github.com/actiontech/dtle/driver/mysql/sql"
	"github.com/actiontech/dtle/g"
	mysql "github.com/go-mysql-org/go-mysql/mysql"
	uuid "github.com/satori/go.uuid"
)

var createTableGtidExecutedV4Query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %v.%v (
job_name varchar(%v) NOT NULL,
source_uuid binary(16) NOT NULL COMMENT 'uuid of the source where the transaction was originally executed.',
gtid bigint NOT NULL COMMENT 'single TX. 0 means the row is for gtid_set',
gtid_set longtext NULL COMMENT 'Meanful when gtid=0. Summary of all GTIDs',
primary key (job_name, source_uuid, gtid))`,
	g.DtleSchemaName, g.GtidExecutedTableV4, g.JobNameLenLimit)

func (a *GtidExecutedCreater) migrateGtidExecutedV2toV3() error {
	a.logger.Info(`migrateGtidExecutedV2toV3 starting`)

	var err error
	var query string

	logErr := func(query string, err error) {
		a.logger.Error(`migrateGtidExecutedV2toV3 failed. manual intervention might be required.`,
			"query", query, "err", err)
	}

	query = fmt.Sprintf("alter table %v.%v rename to %v.%v",
		g.DtleSchemaName, g.GtidExecutedTableV2, g.DtleSchemaName, g.GtidExecutedTempTable2To3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("alter table %v.%v modify column interval_gtid longtext",
		g.DtleSchemaName, g.GtidExecutedTempTable2To3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("alter table %v.%v rename to %v.%v",
		g.DtleSchemaName, g.GtidExecutedTempTable2To3, g.DtleSchemaName, g.GtidExecutedTableV3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	a.logger.Info(`migrateGtidExecutedV2toV3 done`)

	return nil
}
func (a *GtidExecutedCreater) migrateGtidExecutedV3toV4() (err error) {
	a.logger.Info(`migrateGtidExecutedV3toV4 starting`)

	var query string

	logErr := func(query string, err error) {
		a.logger.Error(`migrateGtidExecutedV3toV4 failed. manual intervention might be required.`,
			"query", query, "err", err)
	}

	_, err = a.db.Exec(createTableGtidExecutedV4Query)
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("select hex(job_uuid), source_uuid, interval_gtid from %v.%v",
		g.DtleSchemaName, g.GtidExecutedTableV3)
	rows, err := a.db.Query(query)
	if err != nil {
		logErr(query, err)
		return err
	}
	tx, err := a.db.Begin()
	if err != nil {
		logErr(query, err)
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(fmt.Sprintf("insert into %v.%v values (?, ?, ?, null)",
		g.DtleSchemaName, g.GtidExecutedTableV4))
	if err != nil {
		logErr(query, err)
		return err
	}
	for rows.Next() {
		var job uuid.UUID
		var sid uuid.UUID
		var interval string
		err = rows.Scan(&job, &sid, &interval)
		if err != nil {
			logErr(query, err)
			return err
		}
		if strings.ContainsAny(interval, ",-:") {
			_, err = a.db.Exec(fmt.Sprintf("insert into %v.%v values (?, ?, 0, ?)",
				g.DtleSchemaName, g.GtidExecutedTableV4),
				job.String(), sid.Bytes(), interval)
			if err != nil {
				logErr(query, err)
				return err
			}
		} else {
			gno, err := strconv.ParseInt(interval, 10, 64)
			if err != nil {
				logErr(query, err)
				return err
			}
			_, err = stmt.Exec(job, sid.Bytes(), gno)
			if err != nil {
				logErr(query, err)
				return err
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("drop table %v.%v", g.DtleSchemaName, g.GtidExecutedTableV3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	a.logger.Info(`migrateGtidExecutedV3toV4 done`)

	return nil
}
func (a *GtidExecutedCreater) migrateGtidExecutedV3atoV4() (err error) {
	a.logger.Info(`migrateGtidExecutedV3atoV4 starting`)

	var query string

	logErr := func(query string, err error) {
		a.logger.Error(`migrateGtidExecutedV3atoV4 failed. manual intervention might be required.`,
			"query", query, "err", err)
	}

	_, err = a.db.Exec(createTableGtidExecutedV4Query)
	if err != nil {
		logErr(query, err)
		return err
	}

	_, err = a.db.Exec(fmt.Sprintf("insert into %v.%v (select hex(job_uuid), source_uuid, gtid, gtid_set from %v.%v)",
		g.DtleSchemaName, g.GtidExecutedTableV4, g.DtleSchemaName, g.GtidExecutedTableV3a))

	query = fmt.Sprintf("drop table %v.%v", g.DtleSchemaName, g.GtidExecutedTableV3a)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	a.logger.Info(`migrateGtidExecutedV3atoV4 done`)

	return nil
}
type GtidExecutedCreater struct {
	db *gosql.DB
	logger g.LoggerType
}

func (a *GtidExecutedCreater) createTableGtidExecutedV4() error {
	query := fmt.Sprintf(`
			CREATE DATABASE IF NOT EXISTS %v;
		`, g.DtleSchemaName)
	if _, err := a.db.Exec(query); err != nil {
		return err
	}
	a.logger.Debug("after create dtle schema")

	if result, err := sql.QueryResultData(a.db, fmt.Sprintf("SHOW TABLES FROM %v LIKE '%v%%'",
		g.DtleSchemaName, g.GtidExecutedTempTablePrefix)); nil == err && len(result) > 0 {
		return fmt.Errorf("GtidExecutedTempTable exists. require manual intervention")
	}

	result, err := sql.QueryResultData(a.db, fmt.Sprintf("SHOW TABLES FROM %v LIKE '%v%%'",
		g.DtleSchemaName, g.GtidExecutedTablePrefix))
	if err != nil {
		return err
	}
	a.logger.Debug("after show gtid_executed table", "result", result)

	if len(result) > 1 {
		return fmt.Errorf("multiple GtidExecutedTable exists, while at most one is allowed." +
			" require manual intervention")
	} else if len(result) == 1 {
		switch result[0][0].String {
		case g.GtidExecutedTableV2:
			err = a.migrateGtidExecutedV2toV3()
			if err != nil {
				return err
			}
			err = a.migrateGtidExecutedV3toV4()
			if err != nil {
				return err
			}
			return nil
		case g.GtidExecutedTableV3:
			err = a.migrateGtidExecutedV3toV4()
			if err != nil {
				return err
			}
			return nil
		case g.GtidExecutedTableV3a:
			err = a.migrateGtidExecutedV3atoV4()
			if err != nil {
				return err
			}
			return nil
		case g.GtidExecutedTableV4:
			return nil
		default:
			return fmt.Errorf("newer GtidExecutedTable exists, which is unrecognized by this verion. require manual intervention")
		}
	} else {
		if _, err := a.db.Exec(createTableGtidExecutedV4Query); err != nil {
			return err
		}
		a.logger.Debug("after create gtid_executed table")
		return nil
	}
}

func (a *ApplierIncr) cleanGtidExecuted(sid uuid.UUID, txSid string) error {
	a.logger.Debug("incr. cleanup before WaitForExecution")
	if !a.mtsManager.WaitForAllCommitted() {
		return nil // shutdown
	}
	a.logger.Debug("incr. cleanup after WaitForExecution")

	intervalStr := func() string {
		a.gtidSetLock.RLock()
		defer a.gtidSetLock.RUnlock()
		intervals := base.GetIntervals(a.gtidSet, txSid)
		return base.StringInterval(intervals)
	}()

	// The TX is unnecessary if we first insert and then delete.
	// However, consider `binlog_group_commit_sync_delay > 0`,
	// `begin; delete; insert; commit;` (1 TX) is faster than `insert; delete;` (2 TX)
	dbApplier := a.dbs[0]
	tx, err := dbApplier.Db.BeginTx(a.ctx, &gosql.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	_, err = dbApplier.PsDeleteExecutedGtid.Exec(a.subject, g.UUIDStrToMySQLHex(txSid))
	if err != nil {
		return err
	}

	a.logger.Debug("compactation gtid. new interval", "intervalStr", intervalStr)
	_, err = dbApplier.Db.ExecContext(a.ctx,
		fmt.Sprintf("insert into %v.%v values (?,?,0,?)", g.DtleSchemaName, g.GtidExecutedTableV4),
		a.subject, sid.Bytes(), intervalStr)
	if err != nil {
		return err
	}
	return nil
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (i mysql.Interval, err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop = i.Start + 1
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop, err = strconv.ParseInt(p[1], 10, 64)
		i.Stop = i.Stop + 1
	default:
		err = fmt.Errorf("invalid interval format, must n[-n]")
	}

	if err != nil {
		return
	}

	if i.Stop <= i.Start {
		err = fmt.Errorf("invalid interval format, must n[-n] and the end must >= start")
	}

	return
}

// return: normalized GtidSet
func SelectAllGtidExecuted(db sql.QueryAble, jid string, gtidSet *mysql.MysqlGTIDSet) (
	itemMap base.GtidItemMap, err error) {

	query := fmt.Sprintf(`SELECT source_uuid,gtid,gtid_set FROM %v.%v where job_name=?`,
		g.DtleSchemaName, g.GtidExecutedTableV4)

	rows, err := db.Query(query, jid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	itemMap = make(base.GtidItemMap)

	for rows.Next() {
		var sidUUID uuid.UUID
		var gno int64
		var gnoSet gosql.NullString
		err = rows.Scan(&sidUUID, &gno, &gnoSet)

		if err != nil {
			return nil, err
		}

		item, ok := itemMap[sidUUID]
		if !ok {
			item = &base.GtidItem{
				NRow:      0,
			}
			itemMap[sidUUID] = item
		}
		item.NRow += 1

		if gno == 0 {
			gsetTrimmed := strings.TrimSpace(gnoSet.String)
			if gsetTrimmed == "" {
				gtidSet.AddSet(mysql.NewUUIDSet(sidUUID))
			} else {
				sep := strings.Split(gsetTrimmed, ":")
				// Handle interval
				for i := 0; i < len(sep); i++ {
					if in, err := parseInterval(sep[i]); err != nil {
						return nil, err
					} else {
						gtidSet.AddSet(mysql.NewUUIDSet(sidUUID, in))
					}
				}
			}
		} else {
			gtidSet.AddSet(mysql.NewUUIDSet(sidUUID, mysql.Interval{
				Start: gno,
				Stop:  gno + 1,
			}))
		}
	}

	return itemMap, err
}

