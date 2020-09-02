package queryfrontend

import (
	"time"

	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

type Limits struct {
	MaxQueryLength      time.Duration
	MaxQueryParallelism int
	MaxCacheFreshness   time.Duration
}
type Config struct {
	Limits     Limits
	QueryRange queryrange.Config
	Frontend   frontend.Config
}

func DefaultConfig() Config {
	return Config{
		Limits: Limits{},
		QueryRange: queryrange.Config{
			ResultsCacheConfig: queryrange.ResultsCacheConfig{},
		},
		Frontend: frontend.Config{},
	}
}
