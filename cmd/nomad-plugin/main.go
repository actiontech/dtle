package main

import (
	"flag"
	"fmt"
	dtle "github.com/actiontech/dtle/drivers/mysql"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins"
	"os"
	"runtime"
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
		return dtle.NewDriver(logger)
	})
}
