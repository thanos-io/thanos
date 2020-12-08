package logging

type ReqLogConfig struct {
	HTTP    ProtocolConfigs `yaml:"http"`
	GRPC    ProtocolConfigs `yaml:"grpc"`
	Options OptionsConfig   `yaml:"options"`
}

type ProtocolConfigs struct {
	Options OptionsConfig    `yaml:"options"`
	Config  []ProtocolConfig `yaml:"config"`
}

type OptionsConfig struct {
	Level    string         `yaml:"level"`
	Decision DecisionConfig `yaml:"decision"`
}

type DecisionConfig struct {
	LogStart bool `yaml:"log_start"`
	LogEnd   bool `yaml:"log_end"`
}

type ProtocolConfig struct {
	// The first two options are for HTTP config.
	Path string `yaml:"path"`
	Port uint64 `yaml:"port"`
	// The last two options are for gRPC config.
	Service string `yaml:"service"`
	Method  string `yaml:"method"`
	// Raise an error if all four options are configured
	// Can fill up first two/last option field in yaml
}
