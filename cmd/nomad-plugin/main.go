package main

import (
	"github.com/hashicorp/nomad/plugins"
	"github.com/hashicorp/go-hclog"
	"os"
	dtlemysql "github.com/actiontech/dtle/drivers/mysql"
)

func main() {
	pid := os.Getpid()
	plugins.Serve(func(logger hclog.Logger) interface{} {
		loggerPid := logger.With("pid", pid)
		loggerPid.Warn("plugins.Serve Factory called.")
		return dtlemysql.NewDriver(loggerPid)
	})
}
