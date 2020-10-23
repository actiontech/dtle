package mysql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"github.com/actiontech/dts/internal/client/driver/mysql/base"
	"github.com/actiontech/dts/internal/client/driver/mysql/sql"
	"github.com/actiontech/dts/internal/g"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"strconv"
	"strings"
)

// g.DtleSchemaName will be dynamically set. So we build the query on demand.
func getCreateTableGtidExecutedV3aQuery() string {
	return fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %v.%v (
job_uuid binary(16) NOT NULL,
source_uuid binary(16) NOT NULL COMMENT 'uuid of the source where the transaction was originally executed.',
gtid bigint NOT NULL COMMENT 'single TX. 0 means the row is for gtid_set',
gtid_set longtext NULL COMMENT 'Meanful when gtid=0. Summary of all GTIDs',
primary key (job_uuid, source_uuid, gtid))`,
		g.DtleSchemaName, g.GtidExecutedTableV3a)
}

func (a *Applier) migrateGtidExecutedV2toV3() error {
	a.logger.Info(`migrateGtidExecutedV2toV3 starting`)

	var err error
	var query string

	logErr := func(query string, err error) {
		a.logger.Error(`migrateGtidExecutedV2toV3 failed. manual intervention might be required. query: %v. err: %v`,
			query, err)
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

func (a *Applier) migrateGtidExecutedV3toV3a() (err error) {
	a.logger.Info(`migrateGtidExecutedV3toV3a starting`)

	var query string

	logErr := func(query string, err error) {
		a.logger.Error(`migrateGtidExecutedV3toV3a failed. manual intervention might be required. query: %v. err: %v`,
			query, err)
	}

	_, err = a.db.Exec(getCreateTableGtidExecutedV3aQuery())
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("select job_uuid, source_uuid, interval_gtid from %v.%v",
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
		g.DtleSchemaName, g.GtidExecutedTableV3a))
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
				g.DtleSchemaName, g.GtidExecutedTableV3a),
				job.Bytes(), sid.Bytes(), interval)
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
			_, err = stmt.Exec(job.Bytes(), sid.Bytes(), gno)
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

	a.logger.Info(`migrateGtidExecutedV3toV3a done`)

	return nil
}

func (a *Applier) createTableGtidExecutedV3a() error {
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
	a.logger.Debug("mysql.applier. after show gtid_executed table", "result", result)

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
			err = a.migrateGtidExecutedV3toV3a()
			if err != nil {
				return err
			}
			return nil
		case g.GtidExecutedTableV3:
			err = a.migrateGtidExecutedV3toV3a()
			if err != nil {
				return err
			}
			return nil
		case g.GtidExecutedTableV3a:
			return nil
		default:
			return fmt.Errorf("newer GtidExecutedTable exists, which is unrecognized by this verion. require manual intervention")
		}
	} else {
		if _, err := a.db.Exec(getCreateTableGtidExecutedV3aQuery()); err != nil {
			return err
		}
		a.logger.Debug("mysql.applier. after create gtid_executed table")
		return nil
	}
}

func (a *Applier) cleanGtidExecuted(sid uuid.UUID, intervalStr string) error {
	a.logger.Debug("mysql.applier. incr. cleanup before WaitForExecution")
	if !a.mtsManager.WaitForAllCommitted() {
		return nil // shutdown
	}
	a.logger.Debug("mysql.applier. incr. cleanup after WaitForExecution")

	// The TX is unnecessary if we first insert and then delete.
	// However, consider `binlog_group_commit_sync_delay > 0`,
	// `begin; delete; insert; commit;` (1 TX) is faster than `insert; delete;` (2 TX)
	dbApplier := a.dbs[0]
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
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

	_, err = dbApplier.PsDeleteExecutedGtid.Exec(sid.Bytes())
	if err != nil {
		return err
	}

	a.logger.Debug("mysql.applier: compactation gtid. new interval: %v", intervalStr)
	_, err = dbApplier.Db.ExecContext(context.TODO(),
		fmt.Sprintf("insert into %v.%v values (?,?,0,?)", g.DtleSchemaName, g.GtidExecutedTableV3a),
		a.subjectUUID.Bytes(), sid.Bytes(), intervalStr)
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
func SelectAllGtidExecuted(db sql.QueryAble, jid uuid.UUID) (gtidSet base.GtidSet, err error) {

	query := fmt.Sprintf(`SELECT source_uuid,gtid,gtid_set FROM %v.%v where job_uuid=?`,
		g.DtleSchemaName, g.GtidExecutedTableV3a)

	rows, err := db.Query(query, jid.Bytes())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	gtidSet = make(base.GtidSet)

	for rows.Next() {
		var sidUUID uuid.UUID
		var gno int64
		var gnoSet gosql.NullString
		err = rows.Scan(&sidUUID, &gno, &gnoSet)

		if err != nil {
			return nil, err
		}

		item, ok := gtidSet[sidUUID]
		if !ok {
			item = &base.GtidExecutedItem{
				NRow:      0,
				Intervals: nil,
			}
			gtidSet[sidUUID] = item
		}
		item.NRow += 1
		if gno == 0 {
			sep := strings.Split(gnoSet.String, ":")
			// Handle interval
			for i := 0; i < len(sep); i++ {
				if in, err := parseInterval(sep[i]); err != nil {
					return nil, err
				} else {
					item.Intervals = append(item.Intervals, in)
				}
			}
		} else {
			item.Intervals = append(item.Intervals, mysql.Interval{
				Start: gno,
				Stop:  gno + 1,
			})
		}
	}

	for sid, item := range gtidSet {
		gtidSet[sid].Intervals = item.Intervals.Normalize()
	}

	return gtidSet, err
}
