package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/actiontech/dtle/drivers/api"

	dtle "github.com/actiontech/dtle/drivers/mysql"
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

		logger.Info("dtle starting", "version", versionStr, "pid", pid)
		logger.Info("env", "GODEBUG", os.Getenv("GODEBUG"), "GOMAXPROCS", runtime.GOMAXPROCS(0))
		logger.Debug("plugins.Serve Factory called.")
		go g.DumpLoop(logger)

		dtle.RegisterSetupApiServerFn(api.SetupApiServer)
		return dtle.NewDriver(logger)
	})
}
