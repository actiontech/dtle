node_name = "consul0.localdomain"
data_dir = "/var/lib/consul"

server = true
bootstrap_expect = 1
addresses = {
  http = "0.0.0.0"
}
advertise_addr = "127.0.0.1"

ports = {
  http = 8500
}

log_level = "INFO"
ui = true
disable_update_check = true
