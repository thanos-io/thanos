// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

type RequestConfig struct {
	HTTP    HTTPProtocolConfigs `yaml:"http"`
	GRPC    GRPCProtocolConfigs `yaml:"grpc"`
	Options OptionsConfig       `yaml:"options"`
}

type HTTPProtocolConfigs struct {
	Options OptionsConfig        `yaml:"options"`
	Config  []HTTPProtocolConfig `yaml:"config"`
}

type GRPCProtocolConfigs struct {
	Options OptionsConfig        `yaml:"options"`
	Config  []GRPCProtocolConfig `yaml:"config"`
}

type OptionsConfig struct {
	Level    string         `yaml:"level"`
	Decision DecisionConfig `yaml:"decision"`
}

type DecisionConfig struct {
	LogStart bool `yaml:"log_start"`
	LogEnd   bool `yaml:"log_end"`
}

type HTTPProtocolConfig struct {
	Path string `yaml:"path"`
	Port uint64 `yaml:"port"`
}

type GRPCProtocolConfig struct {
	Service string `yaml:"service"`
	Method  string `yaml:"method"`
}
