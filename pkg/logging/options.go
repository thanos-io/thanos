// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Decision defines rules for enabling start and end of logging.
type Decision int

const (
	// NoLogCall - Logging is disabled.
	NoLogCall Decision = iota
	// LogFinishCall - Only finish logs of request is enabled.
	LogFinishCall
	// LogStartAndFinishCall - Logging of start and end of request is enabled.
	LogStartAndFinishCall
)

var defaultOptions = &options{
	shouldLog:         DefaultDeciderMethod,
	codeFunc:          DefaultErrorToCode,
	levelFunc:         DefaultCodeToLevel,
	durationFieldFunc: DurationToTimeMillisFields,
}

func evaluateOpt(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	optCopy.levelFunc = DefaultCodeToLevel
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

// WithDecider customizes the function for deciding if the HTTP Middlewares/Tripperwares should log.
func WithDecider(f Decider) Option {
	return func(o *options) {
		o.shouldLog = f
	}
}

// WithLevels customizes the function for mapping HTTP response codes and interceptor log level statements.
func WithLevels(f CodeToLevel) Option {
	return func(o *options) {
		o.levelFunc = f
	}
}

// Interface for the additional methods.

// Types for the Options.
type Option func(*options)

// Fields represents logging fields. It has to have even number of elements (pairs).
type Fields []string

// ErrorToCode function determines the error code of the error
// for the http response.
type ErrorToCode func(err error) int

func DefaultErrorToCode(_ error) int {
	return 500
}

// Decider function defines rules for suppressing the logging.
// TODO : Add the method name as a parameter in case we want to log based on method names.
type Decider func() Decision

// DefaultDeciderMethod is the default implementation of decider to see if you should log the call
// by default this is set to LogStartAndFinishCall.
func DefaultDeciderMethod() Decision {
	return LogStartAndFinishCall
}

// CodeToLevel function defines the mapping between HTTP Response codes to log levels for server side.
type CodeToLevel func(logger log.Logger, code int) log.Logger

// DurationToFields function defines how to produce duration fields for logging.
type DurationToFields func(duration time.Duration) Fields

type options struct {
	levelFunc         CodeToLevel
	shouldLog         Decider
	codeFunc          ErrorToCode
	durationFieldFunc DurationToFields
}

// DefaultCodeToLevel is the helper mapper that maps HTTP Response codes to log levels.
func DefaultCodeToLevel(logger log.Logger, code int) log.Logger {
	if code >= 200 && code < 500 {
		return level.Debug(logger)
	}
	return level.Error(logger)
}

// DurationToTimeMillisFields converts the duration to milliseconds and uses the key `http.time_ms`.
func DurationToTimeMillisFields(duration time.Duration) Fields {
	return Fields{"http.time_ms", fmt.Sprintf("%v", durationToMilliseconds(duration))}
}

func durationToMilliseconds(duration time.Duration) float32 {
	return float32(duration.Nanoseconds()/1000) / 1000
}

// LogDecision defines mapping of flag options to the logging decision.
var LogDecision = map[string]Decision{
	"NoLogCall":             NoLogCall,
	"LogFinishCall":         LogFinishCall,
	"LogStartAndFinishCall": LogStartAndFinishCall,
}
