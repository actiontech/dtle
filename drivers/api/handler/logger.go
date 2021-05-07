package handler

import (
	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
)

func NewLogger() hclog.Logger {
	newLogger := g.Logger.Named("http_api")
	return newLogger
}
