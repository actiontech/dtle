// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package terror

import (
	"fmt"
	"io"

	"github.com/pingcap/errors"
)

const (
	errBaseFormat = "[code=%d:class=%s:scope=%s:level=%s]"
)

// ErrCode is used as the unique identifier of a specific error type.
type ErrCode int

// ErrClass represents a class of errors.
type ErrClass int

// Error classes.
const (
	ClassDatabase ErrClass = iota + 1
	ClassFunctional
	ClassConfig
	ClassBinlogOp
	ClassCheckpoint
	ClassTaskCheck
	ClassSourceCheck
	ClassRelayEventLib
	ClassRelayUnit
	ClassDumpUnit
	ClassLoadUnit
	ClassSyncUnit
	ClassDMMaster
	ClassDMWorker
	ClassDMTracer
	ClassSchemaTracker
	ClassScheduler
	ClassDMCtl
	ClassNotSet
	ClassOpenAPI
)

var errClass2Str = map[ErrClass]string{
	ClassDatabase:      "database",
	ClassFunctional:    "functional",
	ClassConfig:        "config",
	ClassBinlogOp:      "binlog-op",
	ClassCheckpoint:    "checkpoint",
	ClassTaskCheck:     "task-check",
	ClassSourceCheck:   "source-check",
	ClassRelayEventLib: "relay-event-lib",
	ClassRelayUnit:     "relay-unit",
	ClassDumpUnit:      "dump-unit",
	ClassLoadUnit:      "load-unit",
	ClassSyncUnit:      "sync-unit",
	ClassDMMaster:      "dm-master",
	ClassDMWorker:      "dm-worker",
	ClassDMTracer:      "dm-tracer",
	ClassSchemaTracker: "schema-tracker",
	ClassScheduler:     "scheduler",
	ClassDMCtl:         "dmctl",
	ClassNotSet:        "not-set",
	ClassOpenAPI:       "openapi",
}

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	if s, ok := errClass2Str[ec]; ok {
		return s
	}
	return fmt.Sprintf("unknown error class: %d", ec)
}

// ErrScope represents the error occurs environment, such as upstream DB error,
// downstream DB error, DM internal error etc.
type ErrScope int

// Error scopes.
const (
	ScopeNotSet ErrScope = iota
	ScopeUpstream
	ScopeDownstream
	ScopeInternal
)

var errScope2Str = map[ErrScope]string{
	ScopeNotSet:     "not-set",
	ScopeUpstream:   "upstream",
	ScopeDownstream: "downstream",
	ScopeInternal:   "internal",
}

// String implements fmt.Stringer interface.
func (es ErrScope) String() string {
	if s, ok := errScope2Str[es]; ok {
		return s
	}
	return fmt.Sprintf("unknown error scope: %d", es)
}

// ErrLevel represents the emergency level of a specific error type.
type ErrLevel int

// Error levels.
const (
	LevelLow ErrLevel = iota + 1
	LevelMedium
	LevelHigh
)

var errLevel2Str = map[ErrLevel]string{
	LevelLow:    "low",
	LevelMedium: "medium",
	LevelHigh:   "high",
}

// String implements fmt.Stringer interface.
func (el ErrLevel) String() string {
	if s, ok := errLevel2Str[el]; ok {
		return s
	}
	return fmt.Sprintf("unknown error level: %d", el)
}

// Error implements error interface and add more useful fields.
type Error struct {
	code       ErrCode
	class      ErrClass
	scope      ErrScope
	level      ErrLevel
	message    string
	workaround string
	args       []interface{}
	rawCause   error
	stack      errors.StackTracer
}

// New creates a new *Error instance.
func New(code ErrCode, class ErrClass, scope ErrScope, level ErrLevel, message string, workaround string) *Error {
	return &Error{
		code:       code,
		class:      class,
		scope:      scope,
		level:      level,
		message:    message,
		workaround: workaround,
	}
}

// Code returns ErrCode.
func (e *Error) Code() ErrCode {
	return e.code
}

// Class returns ErrClass.
func (e *Error) Class() ErrClass {
	return e.class
}

// Scope returns ErrScope.
func (e *Error) Scope() ErrScope {
	return e.scope
}

// Level returns ErrLevel.
func (e *Error) Level() ErrLevel {
	return e.level
}

// Message returns the formatted error message.
func (e *Error) Message() string {
	return e.getMsg()
}

// Workaround returns ErrWorkaround.
func (e *Error) Workaround() string {
	return e.workaround
}

// Error implements error interface.
func (e *Error) Error() string {
	str := fmt.Sprintf(errBaseFormat, e.code, e.class, e.scope, e.level)
	if e.getMsg() != "" {
		str += fmt.Sprintf(", Message: %s", e.getMsg())
	}
	if e.rawCause != nil {
		str += fmt.Sprintf(", RawCause: %s", Message(e.rawCause))
	}
	if e.workaround != "" {
		str += fmt.Sprintf(", Workaround: %s", e.workaround)
	}
	return str
}

// Format accepts flags that alter the printing of some verbs.
func (e *Error) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			//nolint:errcheck
			io.WriteString(s, e.Error())
			if e.stack != nil {
				e.stack.StackTrace().Format(s, verb)
			}
			return
		}
		fallthrough
	case 's':
		//nolint:errcheck
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

// Cause implements causer.Cause defined in pingcap/errors
// and returns the raw cause of an *Error.
func (e *Error) Cause() error {
	return e.rawCause
}

func (e *Error) getMsg() string {
	if len(e.args) > 0 {
		return fmt.Sprintf(e.message, e.args...)
	}
	return e.message
}

// Equal checks if err equals to e.
func (e *Error) Equal(err error) bool {
	if error(e) == err {
		return true
	}
	inErr, ok := err.(*Error)
	return ok && e.code == inErr.code
}

// SetMessage clones an Error and resets its message.
func (e *Error) SetMessage(message string) *Error {
	err := *e
	err.message = message
	err.args = append([]interface{}{}, e.args...)
	return &err
}

// New generates a new *Error with the same class and code, and replace message with new message.
func (e *Error) New(message string) error {
	return e.stackLevelGeneratef(1, message)
}

// Generate generates a new *Error with the same class and code, and new arguments.
func (e *Error) Generate(args ...interface{}) error {
	return e.stackLevelGeneratef(1, e.message, args...)
}

// Generatef generates a new *Error with the same class and code, and a new formatted message.
func (e *Error) Generatef(format string, args ...interface{}) error {
	return e.stackLevelGeneratef(1, format, args...)
}

// stackLevelGeneratef is an inner interface to generate new *Error.
func (e *Error) stackLevelGeneratef(stackSkipLevel int, format string, args ...interface{}) error {
	return &Error{
		code:       e.code,
		class:      e.class,
		scope:      e.scope,
		level:      e.level,
		message:    format,
		workaround: e.workaround,
		args:       args,
		stack:      errors.NewStack(stackSkipLevel),
	}
}

// Delegate creates a new *Error with the same fields of the give *Error,
// except for new arguments, it also sets the err as raw cause of *Error.
func (e *Error) Delegate(err error, args ...interface{}) error {
	if err == nil {
		return nil
	}

	rawCause := err
	// we only get the root rawCause
	if tErr, ok := err.(*Error); ok && tErr.rawCause != nil {
		rawCause = tErr.rawCause
	}

	return &Error{
		code:       e.code,
		class:      e.class,
		scope:      e.scope,
		level:      e.level,
		message:    e.message,
		workaround: e.workaround,
		args:       args,
		rawCause:   rawCause,
		stack:      errors.NewStack(0),
	}
}

// AnnotateDelegate resets the message of *Error and Delegate with error and new args.
func (e *Error) AnnotateDelegate(err error, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return e.SetMessage(message).Delegate(err, args...)
}

// Annotate tries to convert err to *Error and adds a message to it.
// This API is designed to reset Error message but keeps its original trace stack.
func Annotate(err error, message string) error {
	if err == nil {
		return nil
	}
	e, ok := err.(*Error)
	if !ok {
		return errors.Annotate(err, message)
	}
	e.message = fmt.Sprintf("%s: %s", message, e.getMsg())
	e.args = nil
	return e
}

// Annotatef tries to convert err to *Error and adds a message to it.
func Annotatef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	e, ok := err.(*Error)
	if !ok {
		return errors.Annotatef(err, format, args...)
	}
	e.message = fmt.Sprintf("%s: %s", format, e.getMsg())
	e.args = args
	return e
}

// Message returns `getMsg()` value if err is an *Error instance, else returns `Error()` value.
func Message(err error) string {
	if err == nil {
		return ""
	}
	e, ok := err.(*Error)
	if !ok {
		return err.Error()
	}
	return e.getMsg()
}

// WithScope tries to set given scope to *Error, if err is not an *Error instance,
// wrap it with error scope instead.
func WithScope(err error, scope ErrScope) error {
	if err == nil {
		return nil
	}
	e, ok := err.(*Error)
	if !ok {
		return errors.Annotatef(err, "error scope: %s", scope)
	}
	e.scope = scope
	return e
}

// WithClass tries to set given class to *Error, if err is not an *Error instance,
// wrap it with error class instead.
func WithClass(err error, class ErrClass) error {
	if err == nil {
		return nil
	}
	e, ok := err.(*Error)
	if !ok {
		return errors.Annotatef(err, "error class: %s", class)
	}
	e.class = class
	return e
}
