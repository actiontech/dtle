package mysql

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	ubase "udup/internal/client/driver/mysql/base"
	usql "udup/internal/client/driver/mysql/sql"
	uconf "udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
)

const startSlavePostWaitMilliseconds = 500 * time.Millisecond

// Inspector reads data from the read-MySQL-server (typically a replica, but can be the master)
// It is used for gaining initial status and structure, and later also follow up on progress and changelog
type Inspector struct {
	logger       *log.Entry
	db           *gosql.DB
	mysqlContext *uconf.MySQLDriverConfig
}

func NewInspector(ctx *uconf.MySQLDriverConfig, logger *log.Entry) *Inspector {
	return &Inspector{
		logger:       logger,
		mysqlContext: ctx,
	}
}

func (i *Inspector) InitDBConnections() (err error) {
	inspectorUri := i.mysqlContext.ConnectionConfig.GetDBUri()
	if i.db, err = usql.CreateDB(inspectorUri); err != nil {
		return err
	}
	if err := i.validateConnection(); err != nil {
		return err
	}
	if err := i.validateGrants(); err != nil {
		i.logger.Errorf("mysql.inspector: Unexpected error on validateGrants, got %v", err)
		return err
	}
	/*for _, doDb := range i.mysqlContext.ReplicateDoDb {

		for _, doTb := range doDb.Table {
			if err := i.InspectOriginalTable(doDb.Database, doTb); err != nil {
				i.logger.Errorf("mysql.inspector: unexpected error on InspectOriginalTable, got %v", err)
				return err
			}
		}
	}*/

	if err = i.validateGTIDMode(); err != nil {
		return err
	}

	if err := i.validateBinlogs(); err != nil {
		return err
	}
	i.logger.Printf("mysql.inspector: Initiated on %s:%d, version %+v", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port, i.mysqlContext.MySQLVersion)
	return nil
}

func (i *Inspector) ValidateOriginalTable(databaseName, tableName string, table *uconf.Table) (err error) {
	if err := i.validateTable(databaseName, tableName); err != nil {
		return err
	}

	/*if err := i.validateTableForeignKeys(databaseName, tableName, true */ /*this.migrationContext.DiscardForeignKeys*/ /*); err != nil {
		return err
	}*/

	// region UniqueKey
	var uniqueKeys [](*umconf.UniqueKey)
	table.OriginalTableColumns, uniqueKeys, err = i.InspectTableColumnsAndUniqueKeys(databaseName, tableName)
	if err != nil {
		return err
	}

	i.logger.Debugf("table: %s.%s. n_unique_keys: %d", table.TableSchema, table.TableName, len(uniqueKeys))

	for _, uk := range uniqueKeys {
		i.logger.Debugf("A unique key: %s", uk.String())

		ubase.ApplyColumnTypes(i.db, table.TableSchema, table.TableName, &uk.Columns)

		uniqueKeyIsValid := true

		for _, column := range uk.Columns.Columns {
			switch column.Type {
			case umconf.FloatColumnType:
				i.logger.Warning("Will not use %+v as unique key due to FLOAT data type", uk.Name)
				uniqueKeyIsValid = false
			case umconf.JSONColumnType:
				// Noteworthy that at this time MySQL does not allow JSON indexing anyhow, but this code
				// will remain in place to potentially handle the future case where JSON is supported in indexes.
				i.logger.Warnf("Will not use %+v as unique key due to JSON data type", uk.Name)
				uniqueKeyIsValid = false
			default:
				// do nothing
			}
		}

		if uk.HasNullable {
			i.logger.Warnf("Will not use %+v as unique key due to having nullable", uk.Name)
			uniqueKeyIsValid = false
		}

		if !uk.IsPrimary() && "FULL" != i.mysqlContext.BinlogRowImage {
			i.logger.Warnf("Will not use %+v as unique key due to not primary when binlog row image is FULL", uk.Name)
			uniqueKeyIsValid = false
		}

		if uniqueKeyIsValid {
			table.UseUniqueKey = uk
			break
		}
	}
	if table.UseUniqueKey == nil {
		i.logger.Warnf("No valid unique key found for table %s.%s. It will be slow on large table.", table.TableSchema, table.TableName)
	} else {
		i.logger.Infof("Chosen unique key for %s.%s is %s",
			table.TableSchema, table.TableName, table.UseUniqueKey.String())
	}
	// endregion

	if err := i.validateTableTriggers(databaseName, tableName); err != nil {
		return err
	}
	return nil
}

func (i *Inspector) InspectTableColumnsAndUniqueKeys(databaseName, tableName string) (columns *umconf.ColumnList, uniqueKeys [](*umconf.UniqueKey), err error) {
	uniqueKeys, err = i.getCandidateUniqueKeys(databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	/*if len(uniqueKeys) == 0 {
		return columns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}*/
	columns, err = ubase.GetTableColumns(i.db, databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}

	return columns, uniqueKeys, nil
}

// validateConnection issues a simple can-connect to MySQL
func (i *Inspector) validateConnection() error {
	query := `select @@global.version`
	if err := i.db.QueryRow(query).Scan(&i.mysqlContext.MySQLVersion); err != nil {
		return err
	}

	i.logger.Printf("mysql.inspector: Connection validated on %s:%d", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
	return nil
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (i *Inspector) validateGrants() error {
	query := `show grants for current_user()`
	foundAll := false
	foundSuper := false
	foundReplicationClient := false
	foundReplicationSlave := false
	foundDBAll := false

	err := usql.QueryRowsMap(i.db, query, func(rowMap usql.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) {
				foundSuper = true
			}
			if strings.Contains(grant, `REPLICATION CLIENT`) {
				foundReplicationClient = true
			}
			if strings.Contains(grant, `REPLICATION SLAVE`) {
				foundReplicationSlave = true
			}
			if ubase.StringContainsAll(grant, `SELECT`) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	i.mysqlContext.HasSuperPrivilege = foundSuper

	if foundAll {
		i.logger.Printf("mysql.inspector: User has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		i.logger.Printf("mysql.inspector: User has SUPER, REPLICATION SLAVE privileges, and has SELECT privileges")
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		i.logger.Printf("mysql.inspector: User has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has SELECT privileges")
		return nil
	}
	i.logger.Debugf("mysql.inspector: Privileges: super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on *.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, foundDBAll)
	return fmt.Errorf("user has insufficient privileges for extractor. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on *.*")
}

func (i *Inspector) validateGTIDMode() error {
	query := `SELECT @@GTID_MODE`
	var gtidMode string
	if err := i.db.QueryRow(query).Scan(&gtidMode); err != nil {
		return err
	}
	if gtidMode != "ON" {
		return fmt.Errorf("must have GTID enabled: %+v", gtidMode)
	}
	return nil
}

// validateBinlogs checks that binary log configuration is good to go
func (i *Inspector) validateBinlogs() error {
	query := `select @@global.log_bin, @@global.binlog_format`
	var hasBinaryLogs bool
	if err := i.db.QueryRow(query).Scan(&hasBinaryLogs, &i.mysqlContext.BinlogFormat); err != nil {
		return err
	}
	if !hasBinaryLogs {
		return fmt.Errorf("%s:%d must have binary logs enabled", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
	}
	if i.mysqlContext.RequiresBinlogFormatChange() {
		return fmt.Errorf("You must be using ROW binlog format. I can switch it for you, provided --switch-to-rbr and that %s:%d doesn't have replicas", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
	}
	query = `select @@global.binlog_row_image`
	if err := i.db.QueryRow(query).Scan(&i.mysqlContext.BinlogRowImage); err != nil {
		// Only as of 5.6. We wish to support 5.5 as well
		i.mysqlContext.BinlogRowImage = "FULL"
	}
	i.mysqlContext.BinlogRowImage = strings.ToUpper(i.mysqlContext.BinlogRowImage)

	i.logger.Printf("mysql.inspector: Binary logs validated on %s:%d", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
	return nil
}

// validateLogSlaveUpdates checks that binary log log_slave_updates is set. This test is not required when migrating on replica or when migrating directly on master
func (i *Inspector) validateLogSlaveUpdates() error {
	query := `select @@global.log_slave_updates`
	var logSlaveUpdates bool
	if err := i.db.QueryRow(query).Scan(&logSlaveUpdates); err != nil {
		return err
	}

	if logSlaveUpdates {
		i.logger.Printf("mysql.inspector: log_slave_updates validated on %s:%d", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
		return nil
	}

	return fmt.Errorf("%s:%d must have log_slave_updates enabled for executing migration", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
}

// validateTable makes sure the table we need to operate on actually exists
func (i *Inspector) validateTable(databaseName, tableName string) error {
	query := fmt.Sprintf(`show table status from %s like '%s'`, usql.EscapeName(databaseName), tableName)

	tableFound := false
	tableEngine := ""
	err := usql.QueryRowsMap(i.db, query, func(rowMap usql.RowMap) error {
		tableEngine = rowMap.GetString("Engine")
		if rowMap.GetString("Comment") == "VIEW" {
			return fmt.Errorf("%s.%s is a VIEW, not a real table. Bailing out", usql.EscapeName(databaseName), usql.EscapeName(tableName))
		}
		tableFound = true

		return nil
	})
	if err != nil {
		return err
	}
	if !tableFound {
		return fmt.Errorf("Cannot find table %s.%s!", usql.EscapeName(databaseName), usql.EscapeName(tableName))
	}

	return nil
}

// validateTableForeignKeys makes sure no foreign keys exist on the migrated table
func (i *Inspector) validateTableForeignKeys(databaseName, tableName string, allowChildForeignKeys bool) error {
	/*if i.mysqlContext.SkipForeignKeyChecks {
		i.logger.Warnf("--skip-foreign-key-checks provided: will not check for foreign keys")
		return nil
	}*/
	query := `
		SELECT
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA=? AND TABLE_NAME=?) as num_child_side_fk,
			SUM(REFERENCED_TABLE_NAME IS NOT NULL AND REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?) as num_parent_side_fk
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
				REFERENCED_TABLE_NAME IS NOT NULL
				AND ((TABLE_SCHEMA=? AND TABLE_NAME=?)
					OR (REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)
				)
	`
	numParentForeignKeys := 0
	numChildForeignKeys := 0
	err := usql.QueryRowsMap(i.db, query, func(m usql.RowMap) error {
		numChildForeignKeys = m.GetInt("num_child_side_fk")
		numParentForeignKeys = m.GetInt("num_parent_side_fk")
		return nil
	}, databaseName, tableName, databaseName, tableName, databaseName, tableName, databaseName, tableName,
	)
	if err != nil {
		return err
	}
	if numParentForeignKeys > 0 {
		return fmt.Errorf("Found %d parent-side foreign keys on %s.%s. Parent-side foreign keys are not supported. Bailing out", numParentForeignKeys, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	}
	if numChildForeignKeys > 0 {
		if allowChildForeignKeys {
			i.logger.Debugf("Foreign keys found and will be dropped, as per given --discard-foreign-keys flag")
			return nil
		}
		return fmt.Errorf("Found %d child-side foreign keys on %s.%s. Child-side foreign keys are not supported. Bailing out", numChildForeignKeys, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	}
	return nil
}

// validateTableTriggers makes sure no triggers exist on the migrated table
func (i *Inspector) validateTableTriggers(databaseName, tableName string) error {
	query := `
		SELECT COUNT(*) AS num_triggers
			FROM INFORMATION_SCHEMA.TRIGGERS
			WHERE
				TRIGGER_SCHEMA=?
				AND EVENT_OBJECT_TABLE=?
	`
	numTriggers := 0
	err := usql.QueryRowsMap(i.db, query, func(rowMap usql.RowMap) error {
		numTriggers = rowMap.GetInt("num_triggers")

		return nil
	},
		databaseName,
		tableName,
	)
	if err != nil {
		return err
	}
	if numTriggers > 0 {
		return fmt.Errorf("Found triggers on %s.%s. Triggers are not supported at this time. Bailing out", usql.EscapeName(databaseName), usql.EscapeName(tableName))
	}
	return nil
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (i *Inspector) getCandidateUniqueKeys(databaseName, tableName string) (uniqueKeys [](*umconf.UniqueKey), err error) {
	query := `
    SELECT
      COLUMNS.TABLE_SCHEMA,
      COLUMNS.TABLE_NAME,
      COLUMNS.COLUMN_NAME,
      UNIQUES.INDEX_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      COLUMNS.CHARACTER_SET_NAME,
			LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
      has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
      COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
  `
	err = usql.QueryRowsMap(i.db, query, func(m usql.RowMap) error {
		columns := umconf.ParseColumnList(m.GetString("COLUMN_NAMES"))
		uniqueKey := &umconf.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *columns,
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
			LastMaxVals:     make([]string, len(columns.Columns)),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	i.logger.Debugf("mysql.inspector: Potential unique keys in %+v.%+v: %+v", databaseName, tableName, uniqueKeys)
	return uniqueKeys, nil
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func getSharedUniqueKeys(originalUniqueKeys, ghostUniqueKeys [](*umconf.UniqueKey)) (uniqueKeys [](*umconf.UniqueKey), err error) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, originalUniqueKey := range originalUniqueKeys {
		for _, ghostUniqueKey := range ghostUniqueKeys {
			if originalUniqueKey.Columns.EqualsByNames(&ghostUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, originalUniqueKey)
			}
		}
	}
	return uniqueKeys, nil
}

// showCreateTable returns the `show create table` statement for given table
func (i *Inspector) showCreateTable(databaseName, tableName string) (createTableStatement string, err error) {
	var dummy string
	query := fmt.Sprintf(`show create table %s.%s`, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	err = i.db.QueryRow(query).Scan(&dummy, &createTableStatement)
	return createTableStatement, err
}
