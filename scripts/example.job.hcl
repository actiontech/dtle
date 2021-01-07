job "job1" {
  datacenters = ["dc1"]

  group "src" {
    task "src" {
      driver = "dtle"
      config {
        ReplicateDoDb = [{
          TableSchema = "db1"
          Tables = [{
            TableName = "tb1"
          }]
        }]
        GroupMaxSize = 1024 # in bytes, not number of transactions
        GroupTimeout = 100  # in ms
        DropTableIfExists = false
        Gtid = ""
        ChunkSize = 2000
        ConnectionConfig = {
          Host = "127.0.0.1"
          Port = 3307
          User = "root"
          Password = "password"
        }
      }
    }
    restart { # group or task level
      interval = "10m"
      attempts = 3
      delay    = "15s"
      mode     = "delay"
    }
  }
  group "dest" {
    task "dest" {
      driver = "dtle"
      config {
        ConnectionConfig = {
          Host = "127.0.0.1"
          Port = 3308
          User = "root"
          Password = "password"
        }

        # For a kafka job, do not set ConnectionConfig in dest task. Set KafkaConfig instead.
        #KafkaConfig = {
        #  Topic = "kafka1"
        #  Brokers = ["127.0.0.1:9192", "127.0.0.1:9092"]
        #  Converter = "json"
        #}
      }
    }
    restart { # group or task level
      interval = "30m"
      attempts = 3
      delay    = "15s"
      mode     = "delay"
    }
  }

  reschedule {
    # By default, nomad will unlimitedly reschedule a failed task.
    # We limit it to once per 30min here.
    attempts = 1
    interval = "30m"
    unlimited = false
  }
}
