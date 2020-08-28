# Rename for each node
node_name = "consul0"
data_dir = "INSTALL_PREFIX_MAGIC/var/lib/consul"
ui = true

disable_update_check = true

# Address that should be bound to for internal cluster communications
bind_addr = "0.0.0.0"
# Address to which Consul will bind client interfaces, including the HTTP and DNS servers
client_addr = "127.0.0.1"
advertise_addr = "127.0.0.1"
ports = {
  # Customize if necessary. -1 means disable.
  #dns = -1
  #server = 8300
  #http = 8500
  #serf_wan = -1
  #serf_lan = 8301
}

server = true
# For single node
bootstrap_expect = 1

# For 3-node cluster
#bootstrap_expect = 3
#retry_join = ["127.0.0.1", "127.0.0.2", "127.0.0.3"] # will use default serf port

log_level = "INFO"
log_file = "INSTALL_PREFIX_MAGIC/var/log/consul/"
