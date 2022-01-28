// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"fmt"
	"os"
	"sort"
	"text/template"

	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

const UsageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\

{{define "FormatCommands"}}\
{{range .FlattenedCommands}}\
{{if not .Hidden}}\
  {{.FullCommand}}{{if .Default}}*{{end}}{{template "FormatCommand" .}}
{{.Help|Wrap 4}}
{{end}}\
{{end}}\
{{end}}\

{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\

{{end}}\

{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\
{{if .Context.Flags}}\
Flags:
{{alphabeticalSort .Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.SelectedCommand}}\
{{if len .Context.SelectedCommand.Commands}}\
Subcommands:
{{template "FormatCommands" .Context.SelectedCommand}}
{{end}}\
{{else if .App.Commands}}\
Commands:
{{template "FormatCommands" .App}}
{{end}}\
`

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
	app.UsageTemplate(UsageTemplate)
	app.UsageFuncs(template.FuncMap{
		"alphabeticalSort": func(data []*kingpin.FlagModel) []*kingpin.FlagModel {
			sort.Slice(data, func(i, j int) bool { return data[i].Name < data[j].Name })
			return data
		},
	})
	return &App{
		app:        app,
		FlagClause: app,
		setups:     map[string]SetupFunc{},
	}
}

func (a *App) Parse() (cmd string, setup SetupFunc) {
	cmd, err := a.app.Parse(os.Args[1:])
	if err != nil {
		a.app.Usage(os.Args[1:])
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "error parsing commandline arguments: %v", os.Args))
		os.Exit(2)
	}
	return cmd, a.setups[cmd]
}

func (a *App) Command(cmd, help string) AppClause {
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

func (a *appClause) Command(cmd, help string) AppClause {
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
