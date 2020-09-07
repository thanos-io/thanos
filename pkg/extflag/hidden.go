// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extflag

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// HiddenCmdClause returns FlagClause that hides created flags.
func HiddenCmdClause(c FlagClause) FlagClause {
	return hidden{c: c}
}

type hidden struct {
	c FlagClause
}

func (h hidden) Flag(name, help string) *kingpin.FlagClause {
	return h.c.Flag(name, help).Hidden()
}
