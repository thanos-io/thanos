package main

import (
	"github.com/go-kit/kit/log/level"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(app *kingpin.Application, name string) runFunc {
	cmd := app.Command(name, "sidecar for Prometheus server")

	promAddr := cmd.Flag("prometheus.address", "listen address of Prometheus instance").
		Default("localhost:9090").String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").Default("./data").String()

	return func(logger log.Logger, reg prometheus.Registerer) error {
		return runSidecar(logger, reg, *promAddr, *dataDir)
	}
}

func runSidecar(
	logger log.Logger,
	reg prometheus.Registerer,
	promAddr, dataDir string,
) error {
	level.Info(logger).Log("msg", "I'm a sidecar", "promDir", dataDir, "promAddr", promAddr)
	return nil
}
