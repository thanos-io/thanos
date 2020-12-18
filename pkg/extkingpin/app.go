// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type FlagClause interface {
	Flag(name, help string) *kingpin.FlagClause
}

// TODO(bwplotka): Consider some extkingpin package that will not depend on those. Needed: Generics!
type SetupFunc func(*run.Group, log.Logger, *prometheus.Registry, opentracing.Tracer, <-chan struct{}, bool) error

type AppClause interface {
	FlagClause
	Command(cmd string, help string) AppClause
	Flags() []*kingpin.FlagModel
	Setup(s SetupFunc)
}

// App is a wrapper around kingping.Application for easier use.
type App struct {
	FlagClause
	app    *kingpin.Application
	setups map[string]SetupFunc
}

// NewApp returns new App.
func NewApp(app *kingpin.Application) *App {
	app.HelpFlag.Short('h')
	return &App{
		app:        app,
		FlagClause: app,
		setups:     map[string]SetupFunc{},
	}
}

func (a *App) Parse() (cmd string, setup SetupFunc) {
	cmd, err := a.app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "error parsing commandline arguments: %v", os.Args))
		a.app.Usage(os.Args[1:])
		os.Exit(2)
	}
	return cmd, a.setups[cmd]
}

func (a *App) Command(cmd string, help string) AppClause {
	c := a.app.Command(cmd, help)
	return &appClause{
		c:          c,
		FlagClause: c,
		setups:     a.setups,
		prefix:     cmd,
	}
}

type appClause struct {
	c *kingpin.CmdClause

	FlagClause
	setups map[string]SetupFunc
	prefix string
}

func (a *appClause) Command(cmd string, help string) AppClause {
	c := a.c.Command(cmd, help)
	return &appClause{
		c:          c,
		FlagClause: c,
		setups:     a.setups,
		prefix:     a.prefix + " " + cmd,
	}
}

func (a *appClause) Setup(s SetupFunc) {
	a.setups[a.prefix] = s
}

func (a *appClause) Flags() []*kingpin.FlagModel {
	return a.c.Model().Flags
}
