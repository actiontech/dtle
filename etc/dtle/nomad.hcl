name = "nomad0" # Rename for each node.
datacenter = "dc1" # Do NOT change. Unused nomad feature.
data_dir  = "INSTALL_PREFIX_MAGIC/var/lib/nomad"
plugin_dir = "INSTALL_PREFIX_MAGIC/usr/share/dtle/nomad-plugin"

log_level = "Info"
log_file = "INSTALL_PREFIX_MAGIC/var/log/nomad/"

disable_update_check = true

bind_addr = "0.0.0.0"
# Change ports if multiple nodes run on the same machine.
ports {
  http = 4646
  rpc  = 4647
  serf = 4648
}
addresses {
  # Default to `bind_addr`. Or set individually here.
  #http = "127.0.0.1"
  #rpc  = "127.0.0.1"
  #serf = "127.0.0.1"
}
advertise {
  http = "127.0.0.1:4646"
  rpc  = "127.0.0.1:4647"
  serf = "127.0.0.1:4648"
}

server {
  enabled          = true

  bootstrap_expect = 1
  # Set bootstrap_expect to 3 for multiple (high-availablity) nodes.
  # Multiple nomad nodes will join with consul.
}

client {
  enabled = true
  options = {
    "driver.blacklist" = "docker,exec,java,mock,qemu,rawexec,rkt"
  }

  # Will auto join other server with consul.
}

consul {
  # dtle-plugin and nomad itself use consul separately.
  # nomad uses consul for server_auto_join and client_auto_join.
  # Only one consul can be set here. Write the nearest here,
  #   e.g. the one runs on the same machine with the nomad server.
  address = "127.0.0.1:8500"
}

plugin "dtle" {
  config {
    log_level = "Info" # Repeat nomad log level here.
    data_dir = "INSTALL_PREFIX_MAGIC/var/lib/nomad"
    nats_bind = "127.0.0.1:8193"
    nats_advertise = "127.0.0.1:8193"
    # Repeat the consul address above.
    consul = "127.0.0.1:8500"

    # By default, API compatibility layer is disabled.
    api_addr = "127.0.0.1:8190"   # for compatibility API
    nomad_addr = "127.0.0.1:4646" # compatibility API need to access a nomad server
    # rsa_private_key_path indicate the file containing the private key for decrypting mysql password that got through http api
    # rsa_private_key_path = "xxx"

    publish_metrics = false
    stats_collection_interval = 15
  }
}
