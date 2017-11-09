package query

import (
	"time"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/promql"
	"gopkg.in/yaml.v2"
)

type Config struct {
	QueryTimeout         time.Duration `yaml:"query_timeout"`
	MaxConcurrentQueries int           `yaml:"max_conccurent_queries"`
}

func (c Config) EngineOpts(logger log.Logger) *promql.EngineOptions {
	return &promql.EngineOptions{
		Logger:               logger,
		Timeout:              c.QueryTimeout,
		MaxConcurrentQueries: c.MaxConcurrentQueries,
	}
}

func (c Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("<error creating config string: %s>", err)
	}
	return string(b)
}
