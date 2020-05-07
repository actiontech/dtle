name = "nomad0" # rename for each node
datacenter = "dc1"
data_dir  = "/var/lib/nomad"
plugin_dir = "/usr/share/dtle/nomad-plugin"
bind_addr = "0.0.0.0"

disable_update_check = true

ports {
  http = 4646
  rpc  = 4647
  serf = 4648
}
addresses {
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
  enabled       = true
  network_speed = 10
}

plugin "mysql" {
  config {
    NatsBind = "127.0.0.1:8193"
    NatsAdvertise = "127.0.0.1:8193"
    consul = ["127.0.0.1:8500"]
  }
}

consul {
  address = "127.0.0.1:8500"
}

log_level = "Info"
log_file = "/var/log/nomad/"
