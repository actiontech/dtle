#!/usr/bin/env bash
set -e

# Create a temp dir and clean it up on exit
TEMPDIR=`mktemp -d -t udup-test.XXX`
trap "rm -rf $TEMPDIR" EXIT HUP INT QUIT TERM

# Build the Udup binary for the API tests
echo "--> Building udup"
go build -tags "udup_test" -o $TEMPDIR/udup || exit 1

# Run the tests
echo "--> Running tests"
GOBIN="`which go`"
sudo -E PATH=$TEMPDIR:$PATH  -E GOPATH=$GOPATH \
    $GOBIN test -tags "udup_test" ${GOTEST_FLAGS:--cover -timeout=900s} $($GOBIN list ./... | grep -v /vendor/)

