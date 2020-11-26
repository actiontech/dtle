// global values
package g

import (
	"runtime/debug"
	"time"
)

var (
	DtleSchemaName string = "DTLE_BUG_SCHEMA_NOT_SET"
)

const (
	GtidExecutedTempTable2To3   string = "gtid_executed_temp_v2_v3"
	GtidExecutedTempTablePrefix string = "gtid_executed_temp_"
	GtidExecutedTablePrefix     string = "gtid_executed_"
	GtidExecutedTableV2         string = "gtid_executed_v2"
	GtidExecutedTableV3         string = "gtid_executed_v3"
	GtidExecutedTableV3a        string = "gtid_executed_v3a"

	ENV_PRINT_TPS         = "UDUP_PRINT_TPS"
	ENV_DUMP_CHECKSUM     = "DTLE_DUMP_CHECKSUM"
	ENV_DUMP_OLDWAY       = "DTLE_DUMP_OLDWAY"
	ENV_TESTSTUB1_DELAY   = "UDUP_TESTSTUB1_DELAY"
	ENV_FULL_APPLY_DELAY  = "DTLE_FULL_APPLY_DELAY"
	ENV_COUNT_INFO_SCHEMA = "DTLE_COUNT_INFO_SCHEMA"
	ENV_BIG_TX_1M         = "DTLE_BIG_TX_1M"

	LONG_LOG_LIMIT = 256
)


var freeMemoryChan = make(chan struct{}, 0)

func MemoryFreer() {
	for {
		select {
		case <-freeMemoryChan:
			println("menory over limit ,begin to receicer")
			debug.FreeOSMemory()
			time.Sleep(10 * time.Second)
		}
	}
}


func TriggerFreeMemory() {
	select {
	case freeMemoryChan <- struct{}{}:
	default:
	}
}
