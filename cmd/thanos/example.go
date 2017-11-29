package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func registerExample(m map[string]setupFunc, app *kingpin.Application, name string) {
	m[name] = func(g *run.Group, logger log.Logger, metrics *prometheus.Registry, tracer opentracing.Tracer) error {
		// TODO(bplotka): Implement this later, when commands API/flags will be stable.
		return errors.New("Not implemented.")
	}
}
