# hclogvet

`hclogvet` is a `go vet` tool for checking that the Trace/Debug/Info/Warn/Error
methods on `hclog.Logger` are used correctly.

## Usage

This may be used in two ways. It may be invoked directly:

    $ hclogvet .
    /full/path/to/project/log.go:25:8: invalid number of log arguments to Info (1 valid pair only)

Or via `go vet`:

    $ go vet -vettool=$(which hclogvet)
    # full/module/path
    ./log.go:25:8: invalid number of log arguments to Info (1 valid pair only)

## Details

These methods expect an odd number of arguments as in:

    logger.Info("valid login", "account", accountID, "ip", addr)

The leading argument is a message string, and then the remainder of the
arguments are key/value pairs of additional structured data to log to provide
context.

`hclogvet` will detect unfortunate errors like:

    logger.Error("raft request failed: %v", err)
    logger.Error("error opening file", err)
    logger.Debug("too many connections", numConnections, "ip", ipAddr)

So that the author can correct them:

    logger.Error("raft request failed", "error", err)
    logger.Error("error opening file", "error", err)
    logger.Debug("too many connections", "connections", numConnections, "ip", ipAddr)
