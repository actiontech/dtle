name = "nomad0" # rename for each node
datacenter = "dc1"
data_dir  = "/dtle/var/lib/nomad"
plugin_dir = "/dtle/usr/share/dtle/nomad-plugin"

log_level = "Info"
log_file = "/dtle/var/log/nomad/"

disable_update_check = true

bind_addr = "0.0.0.0"
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
}

client {
  enabled = true
  options = {
    "driver.blacklist" = "docker,exec,java,mock,qemu,rawexec,rkt"
  }
}

consul {
  address = "dtle-consul:8500"
}

plugin "dtle" {
  config {
    data_dir = "/dtle/var/lib/nomad"
    nats_bind = "127.0.0.1:8193"
    nats_advertise = "127.0.0.1:8193"
    # Repeat the consul address above.
    consul = "dtle-consul:8500"
    api_addr = "127.0.0.1:8190"   # for compatibility API
    nomad_addr = "127.0.0.1:4646" # compatibility API need to access a nomad server
  }
}
