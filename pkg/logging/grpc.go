// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"sort"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc/status"
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
func getGRPCLoggingOption(logStart, logEnd bool) (grpc_logging.Decision, error) {
	if !logStart && !logEnd {
		return grpc_logging.NoLogCall, nil
	}
	if !logStart && logEnd {
		return grpc_logging.LogFinishCall, nil
	}
	if logStart && logEnd {
		return grpc_logging.LogStartAndFinishCall, nil
	}
	return -1, fmt.Errorf("log start call is not supported")
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

// NewGRPCOption adds in the config options and returns tags for logging middleware.
func NewGRPCOption(configYAML []byte) ([]grpc_logging.Option, error) {

	logOpts := []grpc_logging.Option{
		grpc_logging.WithDecider(func(_ string, _ error) grpc_logging.Decision {
			return grpc_logging.NoLogCall
		}),
		grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
	}

	// Unmarshal YAML.
	// if req logging is disabled.
	if len(configYAML) == 0 {
		return logOpts, nil
	}

	reqLogConfig, err := NewRequestConfig(configYAML)
	// If unmarshalling is an issue.
	if err != nil {
		return logOpts, err
	}

	globalLevel, globalStart, globalEnd, err := fillGlobalOptionConfig(reqLogConfig, true)
	// If global options have invalid entries.
	if err != nil {
		return logOpts, err
	}

	// If the level entry does not matches our entries.
	if err := validateLevel(globalLevel); err != nil {
		return logOpts, err
	}

	// If the combination is valid, use them, otherwise return error.
	reqLogDecision, err := getGRPCLoggingOption(globalStart, globalEnd)
	if err != nil {
		return logOpts, err
	}

	if len(reqLogConfig.GRPC.Config) == 0 {
		logOpts = []grpc_logging.Option{
			grpc_logging.WithDecider(func(_ string, err error) grpc_logging.Decision {

				runtimeLevel := grpc_logging.DefaultServerCodeToLevel(status.Code(err))
				for _, lvl := range MapAllowedLevels[globalLevel] {
					if string(runtimeLevel) == strings.ToLower(lvl) {
						return reqLogDecision
					}
				}
				return grpc_logging.NoLogCall
			}),
			grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
		}
		return logOpts, nil
	}

	logOpts = []grpc_logging.Option{
		grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
	}

	methodNameSlice := []string{}

	for _, eachConfig := range reqLogConfig.GRPC.Config {
		eachConfigMethodName := interceptors.FullMethod(eachConfig.Service, eachConfig.Method)
		methodNameSlice = append(methodNameSlice, eachConfigMethodName)
	}

	logOpts = append(logOpts, []grpc_logging.Option{
		grpc_logging.WithDecider(func(runtimeMethodName string, err error) grpc_logging.Decision {

			idx := sort.SearchStrings(methodNameSlice, runtimeMethodName)
			if idx < len(methodNameSlice) && methodNameSlice[idx] == runtimeMethodName {
				runtimeLevel := grpc_logging.DefaultServerCodeToLevel(status.Code(err))
				for _, lvl := range MapAllowedLevels[globalLevel] {
					if string(runtimeLevel) == strings.ToLower(lvl) {
						return reqLogDecision
					}
				}
			}
			return grpc_logging.NoLogCall
		}),
	}...)
	return logOpts, nil
}
