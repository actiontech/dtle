name = "nomad0" # rename for each node
datacenter = "dc1"
data_dir  = "INSTALL_PREFIX_MAGIC/var/lib/nomad"
plugin_dir = "INSTALL_PREFIX_MAGIC/usr/share/dtle/nomad-plugin"
bind_addr = "0.0.0.0"

disable_update_check = true

# change ports if multiple nodes run on a same machine
ports {
  http = 4646
  rpc  = 4647
  serf = 4648
}
addresses {
  # Default to `bind_addr`. Or set individually here.
  #http = "127.0.0.1:4646"
  #rpc  = "127.0.0.1:4647"
  #serf = "127.0.0.1:4648"
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
  enabled       = true
  options = {
    "driver.blacklist" = "docker,exec,java,mock,qemu,rawexec,rkt"
  }

  # Will auto join other server with consul.
}

plugin "dtle" {
  config {
    data_dir = "INSTALL_PREFIX_MAGIC/var/lib/nomad"
    nats_bind = "127.0.0.1:8193"
    nats_advertise = "127.0.0.1:8193"

    # All consul nodes can be written here.
    consul = ["127.0.0.1:8500"]

    # By default, API compatibility layer is disabled.
    #api_addr = "127.0.0.1:8190"   # for compatibility API
    nomad_addr = "127.0.0.1:4646" # compatibility API need to access a nomad server
  }
}

consul {
  # dtle-plugin and nomad itself use consul separately.
  # nomad uses consul for server_auto_join and client_auto_join.
  # Only one consul can be set here.
  address = "127.0.0.1:8500"
}

log_level = "Info"
log_file = "INSTALL_PREFIX_MAGIC/var/log/nomad/"
