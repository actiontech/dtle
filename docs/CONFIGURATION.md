# Udup Configuration

You can see the latest config file with all available parameters here:
[udup.conf](../etc/udup.conf)

## log Configuration

log_level:Run udup in this log mode.
log_file:Specify the log file name. The empty string means to log to stdout.
log_rotate:

## General Configuration

worker_count:
batch:
nats_addr:
replicate_do_db:
replicate_do_table:

Udup has a few options you can configure under the `replicate_do_table` section of the config.
db_name:
tbl_name:

## Extractor Configuration

The following config parameters are available for extractor:

enabled:
server_id:
conn_cfg:

## Applier Configuration

The following config parameters available for applier.

enabled:
conn_cfg:

## Connection Configuration

Udup has a few options you can configure under the `conn_cfg` section of the config.

host:
port:
user:
password: