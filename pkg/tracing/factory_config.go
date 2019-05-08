package tracing

import "gopkg.in/alecthomas/kingpin.v2"

// FactoryConfig save main trace configs.
type FactoryConfig struct {
	TracingType *string
}

// FactoryConfigFromKingpin create FactoryConfig from kingpin app
func FactoryConfigFromKingpin(app *kingpin.Application) FactoryConfig {
	tracingType := app.Flag("tracing.type", "gcloud/jaeger.").Default("gcloud").String()

	return FactoryConfig{
		TracingType: tracingType,
	}
}
