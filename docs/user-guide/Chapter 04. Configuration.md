Chapter 4. Configuration
===================

You can see the latest config file with all available parameters here:
[udup.conf](../../etc/udup.conf)

##4.1 log Configuration

- log_level:Run udup in this log mode.
- log_file:Specify the log file name. The empty string means to log to stdout.

##4.2 General Configuration

Udup has a few options you can configure under the `General Configuration` section of the config.

- region:Name of the region the Udup agent will be a member of. By default this value is set to "global".
- datacenter:The name of the datacenter this Udup agent is a member of. By default this is set to "dc1".
- bind_addr:The address the agent will bind to for all of its various network services.
- data_dir:DataDir is the directory to store our state in.

##4.3 Ports Configuration

- http (Default 8190):This is used by clients and servers to serve the HTTP API. TCP only.
- rpc (Default 8191):This is used by servers and clients to communicate amongst each other. TCP only.
- serf (Default 8192): This is used by servers to gossip over the WAN to other servers. TCP and UDP.
- nats (Default 8193): This is used by nats clients to other clients to serve the pub/sub msg. TCP only.

##4.4 Addresses Configuration

Addresses is used to override the network addresses we bind to.

- http 
- rpc 
- serf 

##4.5 Advertise Configuration

AdvertiseAddrs is used to control the addresses we advertise out for different network services. All are optional and default to BindAddr and their default Port.

- http 
- rpc 
- serf 

##4.6 Server Configuration

The following config parameters are available for Server:

- enabled:Enabled controls if we are a server.
- bootstrap_expect:BootstrapExpect tries to automatically bootstrap the Consul cluster,by withholding peers until enough servers join.
- heartbeat_grace:HeartbeatGrace is the grace period beyond the TTL to account for network,processing delays and clock skew before marking a node as "down".
- join:Join is a list of addresses to attempt to join when the agent starts. If Serf is unable to communicate with any of these addresses, then the agent will error and exit.
- retry_max:RetryMaxAttempts specifies the maximum number of times to retry joining a host on startup. This is useful for cases where we know the node will be online eventually.
- retry_interval:RetryInterval specifies the amount of time to wait in between join attempts on agent start. The minimum allowed value is 1 second and the default is 30s.

##4.7 Client Configuration

The following config parameters are available for Client:

- enabled:Enable client mode for the agent.
- servers:Servers is a list of known server addresses. These are as "host:port".

##4.8 Metric Configuration

- prometheus_address:Prometheus pushgateway address, leaves it empty will disable prometheus push.
- collection_interval:Prometheus client push interval in second, set \"0\" to disable prometheus push.
- publish_allocation_metrics:PublishAllocationMetrics determines whether udup is going to publish allocation metrics to remote Telemetry sinks
- publish_node_metrics:PublishNodeMetrics determines whether udup is going to publish node level metrics to remote Telemetry sinks

##4.9 Network Configuration

- max_payload(Default 100M):MAX_PAYLOAD is the maximum allowed payload size. Should be using something different if > 100MB payloads are needed.
