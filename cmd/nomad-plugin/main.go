package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/mem"

	"github.com/actiontech/dtle/api"
	"github.com/actiontech/dtle/api/handler"

	dtle "github.com/actiontech/dtle/driver"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins"
)

func main() {
	versionStr := fmt.Sprintf("%v-%v-%v", g.Version, g.GitBranch, g.GitCommit)

	isVersion := flag.Bool("version", false, "Show version")
	flag.Parse()
	if *isVersion {
		fmt.Println(versionStr)
		return
	}

	pid := os.Getpid()
	plugins.Serve(func(logger hclog.Logger) interface{} {
		g.Logger = logger

		vmStat, err := mem.VirtualMemory()
		if err != nil {
			logger.Warn("cannot get available memory. assuming 4096MB")
			g.MemAvailable = 4096 * 1024 * 1024
		} else {
			logger.Info("available memory in MB", "size", vmStat.Available/1024/1024)
			g.MemAvailable = vmStat.Available
		}
		// allow 1 more big-tx job for every 2 GB
		g.BigTxMaxJobs = int32(g.MemAvailable / 1024 / 1024 / 1024 / 2)
		if g.BigTxMaxJobs == 0 {
			g.BigTxMaxJobs = 1
		}

		go func() {
			for {
				time.Sleep(120 * time.Second)
				if g.BigTxReachMax() {
					logger.Info("BigTxReachMax", "value", g.BigTxMaxJobs)
				}
			}
		}()

		logger.Info("dtle starting", "version", versionStr, "pid", pid)
		logger.Info("env", "GODEBUG", os.Getenv("GODEBUG"), "GOMAXPROCS", runtime.GOMAXPROCS(0))
		logger.Debug("plugins.Serve Factory called.")
		go g.DumpLoop()

		d := dtle.NewDriver(logger)
		handler.DtleDriver = d // TODO avoid global var
		d.SetSetupApiServerFn(api.SetupApiServer)
		return d
	})
}
