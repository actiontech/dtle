#Udup
Udup is ...

Design goals are to ...

For more information on Extractor and Applier please [read this](./docs/EXTRACTOR_AND_APPLIER.md).
See the [getting started guide](docs/user-guide/Chapter%2003.%20Installing.md).This guide will get you up and running with Udup. It walks you through the package, installation, and configuration processes, and it shows how to use Udup to extract and apply data with mysql.

## Installation:

### Linux deb and rpm Packages:

Latest:
* dist/udup_a427b6b~a427b6b_amd64.deb
* dist/udup-a427b6b~a427b6b.x86_64.rpm

## Package Instructions:

* Udup binary is installed in `/usr/bin/udup`
* Udup daemon configuration file is in `/etc/udup/udup.conf`
* On sysv systems, the udup daemon can be controlled via
`service udup [action]`
* On systemd systems (such as Ubuntu 15+), the udup daemon can be
controlled via `systemctl [action] udup`

## From Source:

which gets installed via the Makefile
if you don't have it already. You also must build with golang version 1.5+.

1. [Install Go](https://golang.org/doc/install)
2. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
3. Run `git clone git@10.186.18.21:universe/udup.git`
4. Run `cd $GOPATH/src/udup`
5. Run `make prepare & make`

## How to use it:

See usage with:

```
udup --help
```

## Example

```
udup agent -config udup.conf
```

## Configuration

See the [configuration guide](docs/user-guide/Chapter%2004.%20Configuration.md) for a rundown of the more advanced
configuration options.

## Notice

* binlog format must be row.
* binlog row image must be full for MySQL, you may lost some field data if you update PK data in MySQL with minimal or noblob binlog row image.
* only supports the InnoDB storage engine for MySQL (not MyISAM)
* MySQL character set that supports only latin1、latin2、gbk、utf8、utf8mb4、binary

## MySQL privileges
* select @@global.log_bin, @@global.binlog_format;
<br>1 ROW
* select @@global.binlog_row_image;
<br>FULL
* select @@global.log_slave_updates;
<br>1
* SELECT @@GTID_MODE;
<br>ON
* show grants for current_user();
<br>SRC:REPLICATION CLIENT,REPLICATION SLAVE,ALL PRIVILEGES|SUPER
<br>DEST:ALL PRIVILEGES|SUPER

## Todo