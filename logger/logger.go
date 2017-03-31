package logger

import (
	"github.com/Sirupsen/logrus"
)

var Logger = logrus.NewEntry(logrus.New())

func InitLogger(logLevel string, node string) {
	formattedLogger := logrus.New()
	formattedLogger.Formatter = &logrus.TextFormatter{FullTimestamp: true}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.WithError(err).Error("Error parsing log level, using: info")
		level = logrus.InfoLevel
	}

	formattedLogger.Level = level
	Logger = logrus.NewEntry(formattedLogger).WithField("node", node)
}
