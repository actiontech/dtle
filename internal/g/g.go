// global values
package g

import (
	"github.com/shirou/gopsutil/mem"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"sync/atomic"
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


var (
	freeMemoryChan = make(chan struct{}, 0)
	lowMemory = int32(0)
)

func MemoryFreer() {
	for {
		select {
		case <-freeMemoryChan:
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

func IsLowMemory() bool {
	return atomic.LoadInt32(&lowMemory) == 1
}

func MemoryMonitor(logger *logrus.Logger) {
	t := time.NewTicker(1 * time.Second)

	for {
		<-t.C
		memory, err := mem.VirtualMemory()
		if err != nil {
			lowMemory = 0
		} else {
			if (float64(memory.Available)/float64(memory.Total) < 0.2) && (memory.Available < 1*1024*1024*1024) {
				if lowMemory == 0 {
					logger.WithField("available", memory.Available).
						WithField("total", memory.Total).
						Warn("memory is less than 20% and 1GB. pause parsing binlog")
				}
				lowMemory = 1
				TriggerFreeMemory()
			} else {
				if lowMemory == 1 {
					logger.WithField("available", memory.Available).
						WithField("total", memory.Total).
						Info("memory is greater than 20% or 1GB. continue to parse binlog")
				}
				lowMemory = 0
			}
		}
	}
}
