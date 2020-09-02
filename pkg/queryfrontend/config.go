package queryfrontend

import (
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
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
type Config struct {
	Limits     Limits
	QueryRange queryrange.Config
	Frontend   Frontend
}

func DefaultConfig() Config {
	return Config{
		Limits: Limits{},
		QueryRange: queryrange.Config{
			ResultsCacheConfig: queryrange.ResultsCacheConfig{},
		},
		Frontend: Frontend{},
	}
}
