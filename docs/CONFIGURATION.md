# Udup Configuration

You can see the latest config file with all available parameters here:
[udup.conf](../etc/udup.conf)

## log Configuration

log_level:Run udup in this log mode.
log_file:Specify the log file name. The empty string means to log to stdout.
log_rotate:

## General Configuration

Udup has a few options you can configure under the `General Configuration` section of the config.

region:Name of the region the Udup agent will be a member of. By default this value is set to "global".
datacenter:The name of the datacenter this Udup agent is a member of. By default this is set to "dc1".
bind_addr:The address the agent will bind to for all of its various network services.
rpc_port:

## Server Configuration

The following config parameters are available for Server:

server:Enable server mode for the agent.
http_addr:The `address` and port of the Udup HTTP agent

## Client Configuration

The following config parameters are available for Client:

join:Address of an agent to join at start time.

## Nats Configuration

The following config parameters are available for Nats:

nats_addr:
nats_store_type:
nats_file_store_dir:

## Consul Configuration

The following config parameters available for Consul.

addrs:The address to the Consul agent
server_auto_join:Enabling the server to bootstrap using Consul.
client_auto_join:Enabling the client to bootstrap using Consul.