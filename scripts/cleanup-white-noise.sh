#!/bin/bash
SED_BIN=${SED_BIN:-sed}

${SED_BIN} -i 's/[ \t]*$//' "$@"
