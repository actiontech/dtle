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
		i.logger.Errorf("mysql.inspector: unexpected error on validateGrants, got %v", err)
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
	i.logger.Printf("mysql.inspector: initiated on %s:%d, version %+v", i.mysqlContext.ConnectionConfig.Host,i.mysqlContext.ConnectionConfig.Port, i.mysqlContext.MySQLVersion)
	return nil
}

func (i *Inspector) ValidateOriginalTable(databaseName, tableName string) (err error) {
	if err := i.validateTable(databaseName, tableName); err != nil {
		return err
	}

	/*if err := i.validateTableForeignKeys(databaseName, tableName, true */ /*this.migrationContext.DiscardForeignKeys*/ /*); err != nil {
		return err
	}*/

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

func (i *Inspector) InspectOriginalTable(databaseName string, doTb *uconf.Table) (err error) {
	doTb.OriginalTableColumns, doTb.OriginalTableUniqueKeys, err = i.InspectTableColumnsAndUniqueKeys(databaseName, doTb.TableName)
	if err != nil {
		return err
	}
	return nil
}

//It extracts the list of shared columns and the chosen extract unique key
/*func (i *Inspector) inspectTables(databaseName string,doTb *uconf.Table) (err error) {
 */ /*originalNamesOnApplier := doTb.OriginalTableColumnsOnApplier.Names()
originalNames := doTb.OriginalTableColumns.Names()
if !reflect.DeepEqual(originalNames, originalNamesOnApplier) {
	return fmt.Errorf("It seems like table structure is not identical between master and replica. This scenario is not supported.")
}*/ /*

	doTb.SharedColumns, doTb.MappedSharedColumns = i.getSharedColumns(*doTb)
	i.logger.Printf("mysql.inspector: Shared columns are %s", doTb.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	i.applyColumnTypes(databaseName, doTb.Name, doTb.OriginalTableColumns, doTb.SharedColumns)

	for i := range doTb.SharedColumns.ColumnList() {
		column := doTb.SharedColumns.ColumnList()[i]
		mappedColumn := doTb.MappedSharedColumns.ColumnList()[i]
		if column.Name == mappedColumn.Name && column.Type == umconf.DateTimeColumnType && mappedColumn.Type == umconf.TimestampColumnType {
			doTb.MappedSharedColumns.SetConvertDatetimeToTimestamp(column.Name, i.mysqlContext.ApplierTimeZone)
		}
	}

	return nil
}*/

// validateConnection issues a simple can-connect to MySQL
func (i *Inspector) validateConnection() error {
	query := `select @@global.version`
	if err := i.db.QueryRow(query).Scan(&i.mysqlContext.MySQLVersion); err != nil {
		return err
	}

	i.logger.Printf("mysql.inspector: connection validated on %s:%d", i.mysqlContext.ConnectionConfig.Host,i.mysqlContext.ConnectionConfig.Port)
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
			if strings.Contains(grant, "GRANT ALL PRIVILEGES ON *.*") {
				foundDBAll = true
			}
			if ubase.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
				foundDBAll = true
			}
			if ubase.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, " ON *.*") {
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
		i.logger.Printf("mysql.inspector: user has ALL privileges")
		return nil
	}
	if foundSuper && foundReplicationSlave && foundDBAll {
		i.logger.Printf("mysql.inspector: user has SUPER, REPLICATION SLAVE privileges, and has ALL privileges on *.*")
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll {
		i.logger.Printf("mysql.inspector: user has REPLICATION CLIENT, REPLICATION SLAVE privileges, and has ALL privileges on *.*")
		return nil
	}
	i.logger.Debugf("mysql.inspector: privileges: super: %t, REPLICATION CLIENT: %t, REPLICATION SLAVE: %t, ALL on *.*: %t, ALL on *.*: %t", foundSuper, foundReplicationClient, foundReplicationSlave, foundAll, foundDBAll)
	return fmt.Errorf("user has insufficient privileges for migration. Needed: SUPER|REPLICATION CLIENT, REPLICATION SLAVE and ALL on *.*")
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

	i.logger.Printf("mysql.inspector: binary logs validated on %s:%d", i.mysqlContext.ConnectionConfig.Host, i.mysqlContext.ConnectionConfig.Port)
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

// applyColumnTypes
func (i *Inspector) applyColumnTypes(databaseName, tableName string, columnsLists ...*umconf.ColumnList) error {
	query := `
		select
				*
			from
				information_schema.columns
			where
				table_schema=?
				and table_name=?
		`
	err := usql.QueryRowsMap(i.db, query, func(m usql.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		if strings.Contains(columnType, "unsigned") {
			for _, columnsList := range columnsLists {
				columnsList.SetUnsigned(columnName)
			}
		}
		if strings.Contains(columnType, "mediumint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.MediumIntColumnType
			}
		}
		if strings.Contains(columnType, "timestamp") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.TimestampColumnType
			}
		}
		if strings.Contains(columnType, "datetime") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.DateTimeColumnType
			}
		}
		if strings.HasPrefix(columnType, "enum") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.EnumColumnType
			}
		}
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			for _, columnsList := range columnsLists {
				columnsList.SetCharset(columnName, charset)
			}
		}
		return nil
	}, databaseName, tableName)
	return err
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
		uniqueKey := &umconf.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *umconf.ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	i.logger.Debugf("mysql.inspector: potential unique keys in %+v.%+v: %+v", databaseName, tableName, uniqueKeys)
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

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
/*func (i *Inspector) getSharedColumns(doTb uconf.Table) (*umconf.ColumnList, *umconf.ColumnList) {
	columnsInGhost := make(map[string]bool)
	for _, ghostColumn := range doTb.DestTableColumns.Names() {
		columnsInGhost[ghostColumn] = true
	}
	sharedColumnNames := []string{}
	for _, originalColumn := range doTb.OriginalTableColumns.Names() {
		isSharedColumn := false
		if columnsInGhost[originalColumn] || columnsInGhost[doTb.ColumnRenameMap[originalColumn]] {
			isSharedColumn = true
		}
		if doTb.DroppedColumnsMap[originalColumn] {
			isSharedColumn = false
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := doTb.ColumnRenameMap[columnName]; ok {
			mappedSharedColumnNames = append(mappedSharedColumnNames, mapped)
		} else {
			mappedSharedColumnNames = append(mappedSharedColumnNames, columnName)
		}
	}
	return umconf.NewColumnList(sharedColumnNames), umconf.NewColumnList(mappedSharedColumnNames)
}*/

// showCreateTable returns the `show create table` statement for given table
func (i *Inspector) showCreateTable(databaseName, tableName string) (createTableStatement string, err error) {
	var dummy string
	query := fmt.Sprintf(`show create table %s.%s`, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	err = i.db.QueryRow(query).Scan(&dummy, &createTableStatement)
	return createTableStatement, err
}
