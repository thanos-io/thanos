# Project Development Dependencies.

This is directory which stores Go modules with pinned buildable package that is used within this repository, managed by https://github.com/bwplotka/bingo.

* Run `bingo get` to install all tools having each own module file in this directory.
* Run `bingo get <tool>` to install <tool> that have own module file in this directory.
* If `Variables.mk` is present, use $(<upper case tool name>) variable where <tool> is the <root>/.bingo/<tool>.mod.
* See https://github.com/bwplotka/bingo or -h on how to add, remove or change binaries dependencies.

## Requirements

* Go 1.14+
