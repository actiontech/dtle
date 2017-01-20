# Getting started with Udup

This guide will get you up and running with Udup. It walks you through the package, installation, and configuration processes, and it shows how to use Udup to extract and apply data with mysql.

## Package and Install Udup

which gets installed via the Makefile
if you don't have it already. You also must build with golang version 1.5+.

1. [Install Go](https://golang.org/doc/install)
2. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
3. Run `git clone git@10.186.18.21:universe/udup.git`
4. Run `cd $GOPATH/src/udup`
5. Run `make package`

Note: Udup will start automatically using the default configuration when installed from a deb or rpm package.

## Configuration

Configuration file location by installation type
- Linux debian and RPM packages: /etc/udup/udup.conf
- Standalone Binary: see the [configuration guide](./docs/CONFIGURATION.md) for how to create a configuration file

## Start the Udup Server

- Linux (sysvinit and upstart installations)
sudo service udup start
- Linux (systemd installations)
systemctl start udup

## Results

Once Udup is up and running it will start extract data and appling them to the desired datasource.
Returning to our sample configuration, we show what the extractor and applier look like in Udup below. 
```
[info] Loaded configuration from /etc/udup/udup.conf
[info] Udup agent started!
[info] Apply binlog events onto the datasource ...
[info] Extract binlog events from the datasource ...
[info] create BinlogSyncer with config ...
[info] Beginning streaming
``` 