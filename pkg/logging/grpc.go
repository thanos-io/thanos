// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"gopkg.in/yaml.v2"
)

// NewRequestConfig parses the string into a req logging config structure.
// Raise an error if unmarshalling is not possible, or values are not valid.
func NewRequestConfig(configYAML []byte) (*RequestConfig, error) {
	reqLogConfig := &RequestConfig{}
	if err := yaml.UnmarshalStrict(configYAML, reqLogConfig); err != nil {
		return nil, err
	}
	return reqLogConfig, nil
}

// checkOptionsConfigEmpty checks if the OptionsConfig struct is empty and valid.
// If invalid combination is present, return an error.
func checkOptionsConfigEmpty(optcfg OptionsConfig) (bool, error) {
	if optcfg.Level == "" && !optcfg.Decision.LogEnd && !optcfg.Decision.LogStart {
		return true, nil
	}
	if optcfg.Level == "" && (optcfg.Decision.LogStart || optcfg.Decision.LogEnd) {
		return false, fmt.Errorf("level field is empty")
	}
	return false, nil
}

// fillGlobalOptionConfig configures all the method to have global config for logging.
func fillGlobalOptionConfig(reqLogConfig *RequestConfig, isgRPC bool) (string, bool, bool, error) {
	globalLevel := "ERROR"
	globalStart, globalEnd := false, false

	globalOptionConfig := reqLogConfig.Options
	isEmpty, err := checkOptionsConfigEmpty(globalOptionConfig)

	// If the decision for logging is enabled with empty level field.
	if err != nil {
		return "", false, false, err
	}
	if !isEmpty {
		globalLevel = globalOptionConfig.Level
		globalStart = globalOptionConfig.Decision.LogStart
		globalEnd = globalOptionConfig.Decision.LogEnd
	}

	protocolOptionConfig := reqLogConfig.HTTP.Options
	if isgRPC {
		// gRPC config overrides the global config.
		protocolOptionConfig = reqLogConfig.GRPC.Options
	}

	isEmpty, err = checkOptionsConfigEmpty(protocolOptionConfig)
	// If the decision for logging is enabled with empty level field.
	if err != nil {
		return "", false, false, err
	}

	if !isEmpty {
		globalLevel = protocolOptionConfig.Level
		globalStart = protocolOptionConfig.Decision.LogStart
		globalEnd = protocolOptionConfig.Decision.LogEnd
	}
	return globalLevel, globalStart, globalEnd, nil
}

// getGRPCLoggingOption returns the logging ENUM based on logStart and logEnd values.
func getGRPCLoggingOption(logStart, logEnd bool) (grpc_logging.Option, error) {
	if !logStart && !logEnd {
		return grpc_logging.WithLogOnEvents(), nil
	}
	if !logStart && logEnd {
		return grpc_logging.WithLogOnEvents(grpc_logging.FinishCall), nil
	}
	if logStart && logEnd {
		return grpc_logging.WithLogOnEvents(grpc_logging.StartCall, grpc_logging.FinishCall), nil
	}
	return nil, fmt.Errorf("log decision combination is not supported")
}

// validateLevel validates the list of level entries.
// Raise an error if empty or log level not in uppercase.
func validateLevel(level string) error {
	if level == "" {
		return fmt.Errorf("level field in YAML file is empty")
	}
	if level == "INFO" || level == "DEBUG" || level == "ERROR" || level == "WARNING" {
		return nil
	}
	return fmt.Errorf("the format of level is invalid. Expected INFO/DEBUG/ERROR/WARNING, got this %v", level)
}

func InterceptorLogger(l log.Logger) grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(_ context.Context, lvl grpc_logging.Level, msg string, fields ...any) {
		largs := append([]any{"msg", msg}, fields...)
		switch lvl {
		case grpc_logging.LevelDebug:
			_ = level.Debug(l).Log(largs...)
		case grpc_logging.LevelInfo:
			_ = level.Info(l).Log(largs...)
		case grpc_logging.LevelWarn:
			_ = level.Warn(l).Log(largs...)
		case grpc_logging.LevelError:
			_ = level.Error(l).Log(largs...)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}

// NewGRPCOption adds in the config options for logging middleware.
func NewGRPCOption(configYAML []byte) ([]grpc_logging.Option, []string, error) {

	var logOpts []grpc_logging.Option

	// Unmarshal YAML
	// if req logging is disabled.
	if len(configYAML) == 0 {
		return nil, nil, nil
	}

	reqLogConfig, err := NewRequestConfig(configYAML)
	// If unmarshalling is an issue.
	if err != nil {
		return logOpts, nil, err
	}

	globalLevel, globalStart, globalEnd, err := fillGlobalOptionConfig(reqLogConfig, true)
	// If global options have invalid entries.
	if err != nil {
		return logOpts, nil, err
	}
	if !globalStart && !globalEnd {
		//Nothing to do
		return nil, nil, nil
	}

	// If the level entry does not matches our entries.
	if err := validateLevel(globalLevel); err != nil {
		return logOpts, nil, err
	}

	logOpts = []grpc_logging.Option{
		grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
		grpc_logging.WithFieldsFromContext(GetTraceIDAndRequestIDAsField),
	}
	// If the combination is valid, use them, otherwise return error.
	reqLogDecision, err := getGRPCLoggingOption(globalStart, globalEnd)
	if err != nil {
		return logOpts, nil, err
	}

	if len(reqLogConfig.GRPC.Config) == 0 {
		// If no config is present, use the global config.
		logOpts = append(logOpts, reqLogDecision)
		return logOpts, nil, nil
	}

	logOpts = append(logOpts, reqLogDecision)

	var methodNameSlice []string
	for _, eachConfig := range reqLogConfig.GRPC.Config {
		eachConfigMethodName := fmt.Sprintf("/%s/%s", eachConfig.Service, eachConfig.Method)
		methodNameSlice = append(methodNameSlice, eachConfigMethodName)
	}

	return logOpts, methodNameSlice, nil
}
