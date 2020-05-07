package extflag

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// HiddenCmdClause returns CmdClause that hides created flags.
func HiddenCmdClause(c CmdClause) CmdClause {
	return hidden{c: c}
}

type hidden struct {
	c CmdClause
}

func (h hidden) Flag(name, help string) *kingpin.FlagClause {
	return h.c.Flag(name, help).Hidden()
}
