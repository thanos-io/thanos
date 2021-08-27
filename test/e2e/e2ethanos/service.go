// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"os"
	"strconv"

	"github.com/efficientgo/e2e"
)

type Port struct {
	name      string
	portNum   int
	isMetrics bool
}

func NewService(
	e e2e.Environment,
	name string,
	image string,
	command e2e.Command,
	readiness *e2e.HTTPReadinessProbe,
	http, grpc int,
	otherPorts ...Port,
) *e2e.InstrumentedRunnable {
	metricsPorts := "http"
	ports := map[string]int{
		"http": http,
		"grpc": grpc,
	}

	for _, op := range otherPorts {
		ports[op.name] = op.portNum

		if op.isMetrics {
			metricsPorts = op.name
		}
	}

	return e2e.NewInstrumentedRunnable(e, name, ports, metricsPorts).Init(
		e2e.StartOptions{
			Image:            image,
			Command:          command,
			Readiness:        readiness,
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)
}
