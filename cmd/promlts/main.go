package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func init() {
	prometheus.MustRegister(version.NewCollector("prometheus"))
}

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "A block storage based long-term storage for Prometheus")

	app.Version(version.Print("promlts"))
	app.HelpFlag.Short('h')

	cmds := map[string]func() error{
		"sidecar": registerSidecar(app, "sidecar"),
	}

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		app.Usage(os.Args[1:])
		os.Exit(2)
	}
	if err := cmds[cmd](); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "command failed"))
		os.Exit(1)
	}
}
