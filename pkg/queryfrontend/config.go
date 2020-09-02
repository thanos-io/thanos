package queryfrontend

import (
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type Config struct {
	Cache      cache.Config
	Limits     validation.Limits
	QueryRange queryrange.Config
	Frontend   frontend.Config
}

func DefaultConfig() Config {
	return Config{
		Cache:      cache.Config{},
		Limits:     validation.Limits{},
		QueryRange: queryrange.Config{},
		Frontend:   frontend.Config{},
	}
}
