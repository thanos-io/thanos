#!/bin/bash
# Script that checks the code for errors.

GOBIN=${GOBIN:="$GOPATH/bin"}

function print_real_go_files {
    grep --files-without-match 'DO NOT EDIT!' $(find . -iname '*.go') --exclude=./vendor/*
}

function generate_markdown {
    echo "Generating Github markdown"
    oldpwd=$(pwd)
    for i in $(find . -iname 'doc.go' -not -path "*vendor/*"); do
        dir=${i%/*}
        realdir=$(realpath $dir)
        package=${realdir##${GOPATH}/src/}
        echo "$package"
        cd ${dir}
        ${GOBIN}/godoc2ghmd -ex -file DOC.md ${package}
        ln -s DOC.md README.md 2> /dev/null # can fail
        cd ${oldpwd}
    done;
}

go get github.com/devnev/godoc2ghmd
generate_markdown
echo "returning $?"
