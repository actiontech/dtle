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
  }
}
