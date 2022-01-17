// Copyright 2017-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"io"
	"sync"

	natsdLogger "github.com/nats-io/nats-server/v2/logger"
	natsd "github.com/nats-io/nats-server/v2/server"
)

// LogPrefix is prefixed to all NATS Streaming log messages
const LogPrefix = "STREAM: "

// Logger interface for the Streaming project.
// This is an alias of the NATS Server's Logger interface.
type Logger natsd.Logger

// StanLogger is the logger used in this project and implements
// the Logger interface.
type StanLogger struct {
	mu    sync.RWMutex
	ns    *natsd.Server
	debug bool
	trace bool
	ltime bool
	lfile string
	fszl  int64
	ndbg  bool
	ntrc  bool
	log   natsd.Logger
}

// NewStanLogger returns an instance of StanLogger
func NewStanLogger() *StanLogger {
	return &StanLogger{}
}

// SetLogger sets the logger, debug and trace
// DEPRECATED: Use SetLoggerWithOpts instead.
func (s *StanLogger) SetLogger(log Logger, logtime, debug, trace bool, logfile string) {
	s.mu.Lock()
	s.log = log
	s.ltime = logtime
	s.debug = debug
	s.trace = trace
	s.lfile = logfile
	s.mu.Unlock()
}

// SetFileSizeLimit sets the size limit for a logfile
// DEPRECATED: Use SetLoggerWithOpts instead.
func (s *StanLogger) SetFileSizeLimit(limit int64) {
	s.mu.Lock()
	s.fszl = limit
	s.mu.Unlock()
}

// SetLoggerWithOpts sets the logger and various options.
// Note that `debug` and `trace` here are for STAN.
func (s *StanLogger) SetLoggerWithOpts(log Logger, nOpts *natsd.Options, debug, trace bool) {
	s.mu.Lock()
	s.log = log
	s.debug = debug
	s.trace = trace
	s.updateNATSOptions(nOpts)
	s.mu.Unlock()
}

func (s *StanLogger) updateNATSOptions(nOpts *natsd.Options) {
	s.ltime = nOpts.Logtime
	s.lfile = nOpts.LogFile
	s.fszl = nOpts.LogSizeLimit
	s.ndbg = nOpts.Debug
	s.ntrc = nOpts.Trace
}

// UpdateNATSOptions refreshes the NATS related options, for instance after a
// configuration reload, so that if ReopenLogFile() is called, the logger new
// options are applied.
func (s *StanLogger) UpdateNATSOptions(nOpts *natsd.Options) {
	s.mu.Lock()
	s.updateNATSOptions(nOpts)
	s.mu.Unlock()
}

// SetNATSServer allows the logger to have a handle to the embedded NATS Server.
// This sets the logger for the NATS Server and used during logfile re-opening.
func (s *StanLogger) SetNATSServer(ns *natsd.Server) {
	s.mu.Lock()
	s.ns = ns
	ns.SetLogger(s.log, s.ndbg, s.ntrc)
	s.mu.Unlock()
}

// GetLogger returns the logger
func (s *StanLogger) GetLogger() Logger {
	s.mu.RLock()
	l := s.log
	s.mu.RUnlock()
	return l
}

// ReopenLogFile closes and reopen the logfile.
// Does nothing if the logger is not a file based.
func (s *StanLogger) ReopenLogFile() {
	s.mu.Lock()
	if s.lfile == "" {
		s.mu.Unlock()
		s.Noticef("File log re-open ignored, not a file logger")
		return
	}
	if l, ok := s.log.(io.Closer); ok {
		if err := l.Close(); err != nil {
			s.mu.Unlock()
			s.Errorf("Unable to close logger: %v", err)
			return
		}
	}
	// Pass true for debug and trace here. The higher level will suppress the debug/traces if needed.
	fileLog := natsdLogger.NewFileLogger(s.lfile, s.ltime, true, true, true)
	if s.fszl > 0 {
		fileLog.SetSizeLimit(s.fszl)
	}
	if s.ns != nil {
		s.ns.SetLogger(fileLog, s.ndbg, s.ntrc)
	}
	s.log = fileLog
	s.mu.Unlock()
	s.Noticef("File log re-opened")
}

// Close closes this logger, releasing possible held resources.
func (s *StanLogger) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.log.(io.Closer); ok {
		return l.Close()
	}
	return nil
}

// Noticef logs a notice statement
func (s *StanLogger) Noticef(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Noticef(format, v...)
	}, format, v...)
}

// Errorf logs an error
func (s *StanLogger) Errorf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Errorf(format, v...)
	}, format, v...)
}

// Fatalf logs a fatal error
func (s *StanLogger) Fatalf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func (s *StanLogger) Debugf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		// This is running under the protection of StanLogging's lock
		if s.debug {
			log.Debugf(format, v...)
		}
	}, format, v...)
}

// Tracef logs a trace statement
func (s *StanLogger) Tracef(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		if s.trace {
			logger.Tracef(format, v...)
		}
	}, format, v...)
}

// Warnf logs a warning statement
func (s *StanLogger) Warnf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Warnf(format, v...)
	}, format, v...)
}

func (s *StanLogger) executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}) {
	s.mu.Lock()
	if s.log == nil {
		s.mu.Unlock()
		return
	}
	f(s.log, LogPrefix+format, args...)
	s.mu.Unlock()
}
