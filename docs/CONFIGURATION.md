# Udup Configuration

You can see the latest config file with all available parameters here:
[udup.conf](../etc/udup.conf)

## log Configuration

log_level:Run udup in this log mode.
log_file:Specify the log file name. The empty string means to log to stdout.

## General Configuration

Udup has a few options you can configure under the `General Configuration` section of the config.

region:Name of the region the Udup agent will be a member of. By default this value is set to "global".
datacenter:The name of the datacenter this Udup agent is a member of. By default this is set to "dc1".
bind_addr:The address the agent will bind to for all of its various network services.

## Server Configuration

The following config parameters are available for Server:

enabled:Enable server mode for the agent.

## Client Configuration

The following config parameters are available for Client:

enabled:Enable client mode for the agent.

## Nats Configuration

The following config parameters are available for Nats:

nats_addr:
nats_store_type:
nats_file_store_dir:

## Consul Configuration

The following config parameters available for Consul.

address:The address to the Consul agent
server_auto_join:Enabling the server to bootstrap using Consul.
client_auto_join:Enabling the client to bootstrap using Consul.