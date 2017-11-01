package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(app *kingpin.Application, cmd string) func() error {
	app.Command(cmd, "sidecar for Prometheus server")

	return func() error {
		return runSidecar()
	}
}

func runSidecar() error {
	fmt.Println("woah I'm a sidecar")
	return nil
}
