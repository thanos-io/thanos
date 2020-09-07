package queryfrontend

import (
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/pkg/errors"
)

type Limits struct {
	MaxQueryLength      time.Duration `yaml:"max_query_length"`
	MaxQueryParallelism int           `yaml:"max_query_parallelism"`
	MaxCacheFreshness   time.Duration `yaml:"max_cache_freshness"`
}
type Frontend struct {
	CompressResponses    bool          `yaml:"compress_responses"`
	DownstreamURL        string        `yaml:"downstream_url"`
	LogQueriesLongerThan time.Duration `yaml:"log_queries_longer_than"`
}

// Validate validates the config.
func (cfg *Frontend) Validate() error {
	if len(cfg.DownstreamURL) == 0 {
		return errors.New("downstream URL should be configured")
	}
	return nil
}

type QueryRange struct {
	SplitQueriesByInterval time.Duration                 `yaml:"split_queries_by_interval"`
	MaxRetries             int                           `yaml:"max_retries"`
	ResultsCacheConfig     queryrange.ResultsCacheConfig `yaml:"results_cache"`
}

// Validate validates the config.
func (cfg *QueryRange) Validate() error {
	if cfg.ResultsCacheConfig != (queryrange.ResultsCacheConfig{}) {
		if err := cfg.ResultsCacheConfig.CacheConfig.Validate(); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	return nil
}

type Config struct {
	Limits     Limits
	QueryRange QueryRange
	Frontend   Frontend
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	err := errors.Wrapf(cfg.QueryRange.Validate(), "query range config validation")
	err = errors.Wrapf(cfg.Frontend.Validate(), "frontend config validation")
	return err
}
