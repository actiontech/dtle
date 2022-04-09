package handler

import (
	"github.com/actiontech/dtle/g"
)

func NewLogger() g.LoggerType {
	newLogger := g.Logger.Named("http_api")
	return newLogger
}
