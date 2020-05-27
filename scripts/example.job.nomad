job "job1" {
  datacenters = ["dc1"]
  type = "service"
  update {
    stagger      = "30s"
    max_parallel = 2
  }

  group "Src" {
    task "src" {
      driver = "mysql"
      config {
        ReplicateDoDb = [{
          TableSchema = "db1"
          Tables = [{
            TableName = "tb1"
          }]
        }]
        DropTableIfExists = true
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
  group "Dest" {
    task "dest" {
      driver = "mysql"
      config {
        type = kafka
        ConnectionConfig = {
          Host = "127.0.0.1"
          Port = 3308
          User = "root"
          Password = "password"
        }
      }
    }
  }
}
