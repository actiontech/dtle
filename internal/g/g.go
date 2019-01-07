package g

var (
	DtleSchemaName string = "DTLE_BUG_SCHEMA_NOT_SET"
)

const (
	GtidExecutedTempTable2To3   string = "gtid_executed_temp_v2_v3"
	GtidExecutedTempTablePrefix string = "gtid_executed_temp_"
	GtidExecutedTablePrefix     string = "gtid_executed_"
	GtidExecutedTableV2         string = "gtid_executed_v2"
	GtidExecutedTableV3         string = "gtid_executed_v3"

	ENV_PRINT_TPS        = "UDUP_PRINT_TPS"
	ENV_DUMP_CHECKSUM    = "DTLE_DUMP_CHECKSUM"
	ENV_DUMP_OLDWAY      = "DTLE_DUMP_OLDWAY"
	ENV_TESTSTUB1_DELAY  = "UDUP_TESTSTUB1_DELAY"
	ENV_FULL_APPLY_DELAY = "DTLE_FULL_APPLY_DELAY"
)
