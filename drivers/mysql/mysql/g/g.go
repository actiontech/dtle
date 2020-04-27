// global values
package g

import "fmt"

const (
	DtleSchemaName string = "dtle"
	GtidExecutedTempTable2To3   string = "gtid_executed_temp_v2_v3"
	GtidExecutedTempTablePrefix string = "gtid_executed_temp_"
	GtidExecutedTablePrefix     string = "gtid_executed_"
	GtidExecutedTableV2         string = "gtid_executed_v2"
	GtidExecutedTableV3         string = "gtid_executed_v3"
	GtidExecutedTableV4         string = "gtid_executed_v4"

	JobNameLenLimit = 64

	ENV_PRINT_TPS         = "UDUP_PRINT_TPS"
	ENV_DUMP_CHECKSUM     = "DTLE_DUMP_CHECKSUM"
	ENV_DUMP_OLDWAY       = "DTLE_DUMP_OLDWAY"
	ENV_TESTSTUB1_DELAY   = "UDUP_TESTSTUB1_DELAY"
	ENV_FULL_APPLY_DELAY  = "DTLE_FULL_APPLY_DELAY"
	ENV_COUNT_INFO_SCHEMA = "DTLE_COUNT_INFO_SCHEMA"

	LONG_LOG_LIMIT = 256
)

func ValidateJobName(name string) error {
	if len(name) > JobNameLenLimit {
		return fmt.Errorf("job name too long. jobName %v lenLimit %v", name, JobNameLenLimit)
	}
	for _, c := range name {
		switch c {
		case '\'', '"':
			return fmt.Errorf("job name contains invalid char %v", c)
		}
	}
	return nil
}
