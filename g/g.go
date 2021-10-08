// global values
package g

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/shirou/gopsutil/mem"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

var (
	Version   string
	GitBranch string
	GitCommit string
)

type LoggerType hclog.Logger

var Logger LoggerType
var RsaPrivateKey string

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
	ENV_BIG_MSG_100K      = "DTLE_BIG_MSG_100K"
	ENV_SKIP_GTID_EXECUTED_TABLE = "DTLE_SKIP_GTID_EXECUTED_TABLE"
	ENV_FORCE_MTS         = "DTLE_FORCE_MTS"
	NatsMaxPayload        = 64 * 1024 * 1024

	LONG_LOG_LIMIT = 256

	PluginName = "dtle"
)

var (
	// slightly smaller than NatsMaxPayload
	NatsMaxMsg = 64 * 1024 * 1024 - 4096
	HASH_STRING_SEPARATOR_BYTES = []byte{'½'} // from mysql-server rpl_write_set_handler.cc
)

// EnvIsTrue returns true if the env exists and is not "0".
func EnvIsTrue(env string) bool {
	val, exist := os.LookupEnv(env)
	if !exist {
		return false
	}
	return val != "0"
}

func StringPtrEmpty(p *string) bool {
	if p == nil {
		return true
	} else {
		return *p == ""
	}
}

var (
	freeMemoryCh = make(chan struct{})
	freeMemoryWorkerCount = int32(0)
	lowMemory = int32(0)
	memoryMonitorCount = int32(0)
)

func FreeMemoryWorker() {
	if !atomic.CompareAndSwapInt32(&freeMemoryWorkerCount, 0, 1) {
		return
	}
	for {
		select {
		case <-freeMemoryCh:
			debug.FreeOSMemory()
			time.Sleep(10 * time.Second)
		}
	}
}

func TriggerFreeMemory() {
	select {
	case freeMemoryCh <- struct{}{}:
	default:
	}
}

func IsLowMemory() bool {
	//return atomic.LoadInt32(&lowMemory) == 1
	return lowMemory == 1
}

func MemoryMonitor(logger LoggerType) {
	if !atomic.CompareAndSwapInt32(&memoryMonitorCount, 0, 1) {
		return
	}

	t := time.NewTicker(1 * time.Second)

	for {
		<-t.C
		memory, err := mem.VirtualMemory()
		if err != nil {
			lowMemory = 0
		} else {
			if (float64(memory.Available)/float64(memory.Total) < 0.2) && (memory.Available < 1*1024*1024*1024) {
				if lowMemory == 0 {
					logger.Warn("memory is less than 20% and 1GB. pause parsing binlog",
						"available", memory.Available, "total", memory.Total)
				}
				lowMemory = 1
				TriggerFreeMemory()
			} else {
				if lowMemory == 1 {
					logger.Info("memory is greater than 20% or 1GB. continue parsing binlog",
						"available", memory.Available, "total", memory.Total)
				}
				lowMemory = 0
			}
		}
	}
}

func UUIDStrToMySQLHex(u string) string {
	return strings.ToUpper(strings.ReplaceAll(u, "-", ""))
}
