node_name = "consul0.localdomain"
data_dir = "/var/lib/consul"

server = true
bootstrap_expect = 1
addresses = {
  http = "0.0.0.0"
}

log_level = "INFO"
ui = true
disable_update_check = true
