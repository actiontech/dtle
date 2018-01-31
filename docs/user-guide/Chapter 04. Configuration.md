Chapter 4. Configuration
===================

You can see the latest config file with all available parameters here:
[udup.conf](../../etc/udup.conf)

##4.1 log Configuration

- log_level:Run udup in this log mode.
- log_file:Specify the log file name. The empty string means to log to stdout.

##4.2 General Configuration

Udup has a few options you can configure under the `General Configuration` section of the config.

- bind_addr:The address the agent will bind to for all of its various network services.
- data_dir:DataDir is the directory to store our state in.
- ui:Enables the built-in static web UI server.
- ui-dir:Path to directory containing the web UI resources.

##4.3 Ports Configuration

- http (Default 8190):This is used by clients and servers to serve the HTTP API. TCP only.
- rpc (Default 8191):This is used by servers and clients to communicate amongst each other. TCP only.
- serf (Default 8192): This is used by servers to gossip over the WAN to other servers. TCP and UDP.
- nats (Default 8193): This is used by nats clients to other clients to serve the pub/sub msg. TCP only.

##4.6 Manager Configuration

The following config parameters are available for Server:

- enabled:Enabled controls if we are a server.
- heartbeat_grace:HeartbeatGrace is the grace period beyond the TTL to account for network,processing delays and clock skew before marking a node as "down".
- join:Join is a list of addresses to attempt to join when the agent starts. If Serf is unable to communicate with any of these addresses, then the agent will error and exit.
- retry_max:RetryMaxAttempts specifies the maximum number of times to retry joining a host on startup. This is useful for cases where we know the node will be online eventually.
- retry_interval:RetryInterval specifies the amount of time to wait in between join attempts on agent start. The minimum allowed value is 1 second and the default is 30s.

##4.7 Agent Configuration

The following config parameters are available for Client:

- enabled:Enable client mode for the agent.
- managers:Managers is a list of known manager addresses. These are as "ip:port".

##4.8 Metric Configuration

- prometheus_address:Prometheus pushgateway address, leaves it empty will disable prometheus push.
- collection_interval:Prometheus client push interval in second, set \"0\" to disable prometheus push.
- publish_allocation_metrics:PublishAllocationMetrics determines whether udup is going to publish allocation metrics to remote Telemetry sinks
- publish_node_metrics:PublishNodeMetrics determines whether udup is going to publish node level metrics to remote Telemetry sinks

##4.9 Network Configuration

- max_payload(Default 100M):MAX_PAYLOAD is the maximum allowed payload size. Should be using something different if > 100MB payloads are needed.
