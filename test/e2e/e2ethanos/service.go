// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"os"
	"strconv"

	"github.com/efficientgo/e2e"
)

type Port struct {
	Name      string
	PortNum   int
	IsMetrics bool
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
	return newUninitiatedService(e, name, http, grpc, otherPorts...).Init(
		e2e.StartOptions{
			Image:            image,
			Command:          command,
			Readiness:        readiness,
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)
}

func newUninitiatedService(
	e e2e.Environment,
	name string,
	http, grpc int,
	otherPorts ...Port,
) *e2e.FutureInstrumentedRunnable {
	metricsPorts := "http"
	ports := map[string]int{
		"http": http,
		"grpc": grpc,
	}

	for _, op := range otherPorts {
		ports[op.Name] = op.PortNum

		if op.IsMetrics {
			metricsPorts = op.Name
		}
	}

	return e2e.NewInstrumentedRunnable(e, name, ports, metricsPorts)
}

func initiateService(
	service *e2e.FutureInstrumentedRunnable,
	image string,
	command e2e.Command,
	readiness *e2e.HTTPReadinessProbe,
) *e2e.InstrumentedRunnable {
	return service.Init(
		e2e.StartOptions{
			Image:            image,
			Command:          command,
			Readiness:        readiness,
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)
}
