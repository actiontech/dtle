region = "foobar"
datacenter = "dc2"
name = "my-web"
data_dir = "/tmp/udup"
log_level = "ERR"
bind_addr = "192.168.0.1"
enable_debug = true
ports {
	http = 3000
	rpc = 3001
	serf = 3002
	nats = 3003
}
addresses {
	http = "0.0.0.0"
	rpc = "0.0.0.0"
	serf = "0.0.0.0"
	nats = "0.0.0.0"
}
advertise {
	rpc = "127.0.0.3"
	serf = "127.0.0.4"
	nats = "127.0.0.5"
}
client {
	enabled = true
	state_dir = "/tmp/client-state"
	alloc_dir = "/tmp/alloc"
	servers = ["a.b.c:80", "127.0.0.1:1234"]
	node_class = "linux-medium-64bit"
	meta {
		foo = "bar"
		baz = "zip"
	}
	chroot_env {
		"/opt/myapp/etc" = "/etc"
		"/opt/myapp/bin" = "/bin"
	}
	network_interface = "eth0"
	network_speed = 100
	reserved {
		cpu = 10
		memory = 10
		disk = 10
		iops = 10
		reserved_ports = "1,100,10-12"
	}
	client_min_port = 1000
	client_max_port = 2000
    max_kill_timeout = "10s"
    stats {
        data_points = 35
        collection_interval = "5s"
    }
}
server {
	enabled = true
	bootstrap_expect = 5
	data_dir = "/tmp/data"
	protocol_version = 3
	num_schedulers = 2
	enabled_schedulers = ["test"]
	node_gc_threshold = "12h"
	heartbeat_grace   = "30s"
	retry_join = [ "1.1.1.1", "2.2.2.2" ]
	start_join = [ "1.1.1.1", "2.2.2.2" ]
	retry_max = 3
	retry_interval = "15s"
	rejoin_after_leave = true
}
leave_on_interrupt = true
leave_on_terminate = true
enable_syslog = true
syslog_facility = "LOCAL1"
disable_update_check = true
disable_anonymous_signature = true
http_api_response_headers {
	Access-Control-Allow-Origin = "*"
}