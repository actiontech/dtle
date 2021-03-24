// global values
package g

import (
	hclog "github.com/hashicorp/go-hclog"
)

var (
	Version   string
	GitBranch string
	GitCommit string
)

var Logger hclog.Logger

const (
	DtleSchemaName string = "dtle"
	GtidExecutedTempTable2To3   string = "gtid_executed_temp_v2_v3"
	GtidExecutedTempTablePrefix string = "gtid_executed_temp_"
	GtidExecutedTablePrefix     string = "gtid_executed_"
	GtidExecutedTableV2         string = "gtid_executed_v2"
	GtidExecutedTableV3         string = "gtid_executed_v3"
	GtidExecutedTableV3a        string = "gtid_executed_v3a"
	GtidExecutedTableV4         string = "gtid_executed_v4"

	JobNameLenLimit = 64

	ENV_PRINT_TPS         = "UDUP_PRINT_TPS"
	ENV_DUMP_CHECKSUM     = "DTLE_DUMP_CHECKSUM"
	ENV_DUMP_OLDWAY       = "DTLE_DUMP_OLDWAY"
	ENV_TESTSTUB1_DELAY   = "UDUP_TESTSTUB1_DELAY"
	ENV_FULL_APPLY_DELAY  = "DTLE_FULL_APPLY_DELAY"
	ENV_COUNT_INFO_SCHEMA = "DTLE_COUNT_INFO_SCHEMA"
	ENV_BIG_MSG_1M         = "DTLE_BIG_MSG_1M"

	LONG_LOG_LIMIT = 256

	PluginName = "dtle"

	NatsMaxPayload = 200 * 1024 * 1024
)

func StringPtrEmpty(p *string) bool {
	if p == nil {
		return true
	} else {
		return *p == ""
	}
}
