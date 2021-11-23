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
	lowMemory = false
	memoryMonitorCount = int32(0)
	MemAvailable = uint64(0)
	bigTxJobs    = int32(0)
	BigTxMaxJobs = int32(1)
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
	memory, err := mem.VirtualMemory()
	if err != nil {
		return false
	}
	if ((memory.Available * 10) < (memory.Total * 2)) && (memory.Available < 1*1024*1024*1024) {
		if !lowMemory {
			Logger.Warn("memory is less than 20% and 1GB. pause parsing binlog",
				"available", memory.Available, "total", memory.Total)
		}
		lowMemory = true
		return true
	} else {
		if lowMemory {
			Logger.Info("memory is greater than 20% or 1GB. continue parsing binlog",
				"available", memory.Available, "total", memory.Total)
		}
		lowMemory = false
		return false
	}
}

func MemoryMonitor(logger LoggerType) {
	if !atomic.CompareAndSwapInt32(&memoryMonitorCount, 0, 1) {
		return
	}

	t := time.NewTicker(1 * time.Second)

	for {
		<-t.C
		if IsLowMemory() {
			TriggerFreeMemory()
		} else {

		}
	}
}

func UUIDStrToMySQLHex(u string) string {
	return strings.ToUpper(strings.ReplaceAll(u, "-", ""))
}

func LowerString(s *string) {
	*s = strings.ToLower(*s)
}

func AddBigTxJob() {
	nv := atomic.AddInt32(&bigTxJobs, 1)
	if nv >= BigTxMaxJobs {
		Logger.Debug("big tx job number reaches max", "jobs", nv, "max", BigTxMaxJobs)
	}
}
func SubBigTxJob() {
	nv := atomic.AddInt32(&bigTxJobs, -1)
	if nv == BigTxMaxJobs - 1 {
		Logger.Debug("big tx job number decreased", "jobs", nv, "max", BigTxMaxJobs)
	}
}
func BigTxReachMax() bool {
	return atomic.LoadInt32(&bigTxJobs) >= BigTxMaxJobs
}
