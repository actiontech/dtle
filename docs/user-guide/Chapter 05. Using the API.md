# Udup 接口

<a name="overview"></a>
## 概览

Udup 通过 http 实现一个 rest 风格的 json api 来与软件客户端进行通信。默认情况下, Udup 监听端口 `8190`。本节中的所有示例都假定您使用的是默认端口。

### 版本信息
*版本* : 0.3.0

### 请求信息
*Host* : localhost:8190  
*BasePath* : /v1  
*Schemes* : HTTP

### Consumes

* `application/json`

### Produces

* `application/json`

<a name="paths"></a>
## API接口一览

### POST /jobs
## 1. 接口描述
该接口于创建数据同步/迁移任务，返回任务的创建结果。

## 2. 输入参数
以下请求参数列表仅列出了接口请求参数

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| ID | 否 | Int | 数据复制任务ID，请使用查询数据复制任务列表接口查询任务ID |
| Name | 是 | String | 数据复制任务名称 |
| Type | 是 | String | 数据复制作业类型（同步/迁移/消息订阅） |
| Tasks | 是 | Array | 数据复制作业的任务集合 |

其中， Tasks 中每一个元素为Object，其构成如下：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| Type | 是 | String | 数据复制任务类型（抽取/回放）,可取值包括：<br>Src-源MySQL实例（主实例）<br>Dest-目的MySQL实例（灾备实例） |
| Driver | 是 | String | 数据复制对象类型,可取值包括：<br>MySQL<br>Oracle |
| NodeId | 否 | String | 指定任务节点ID，可使用[查询节点列表](/docs/api/) 接口获取，其值为输出参数中字段 id 的值。 |
| Config | 是 | Object | 配置信息 |

Config 为该任务中数据相关的配置，字段描述为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| Gtid | 否 | String | MySQL Gtid位置 |
| NatsAddr | 是 | String | 数据传输地址 |
| ParallelWorkers | 否 | Int | 并行回放数 |
| ReplChanBufferSize | 否 | Int | 复制任务缓存限制 |
| MsgBytesLimit | 否 | Int | 传输消息大小限制 |
| ReplicateDoDb | 否 | Array | 需要同步的源数据库表信息，如果您需要同步的是整个实例，该字段可不填写，每个元素具体构成见下表 |
| ConnectionConfig | 是 | Object | 数据源连接信息 |

其中， ConnectionConfig 的构成为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| Host | 是 | String | 数据源主机 |
| Port | 是 | Int | 数据源端口 |
| User | 是 | String | 数据源帐号 |
| Password | 是 | String | 数据源密码 |

其中， ReplicateDoDb 可指定需要同步的数据库表信息，数组中的每个元素为Object，其构成如下：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| TableSchema | 否 | String | 数据库名
| Tables | 否 | Array | 当前数据库下的表名，如果您需要同步的是当前数据库的所有表，该字段可不填写

其中， Tables 的构成为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| TableName | 否 | String | 数据复制表对象名

## 3. 输出参数
| 参数名称 | 类型 | 描述 |
|---------|---------|---------|
| Success | Bool | 返回结果 true/false |

## 4. 示例
输入
```` json
 {
     "Name": "exam-7-9", 
     "Type": "synchronous",  
     "Tasks": [
         {
             "Type": "Src", 
             "NodeId": "1eda45f8-df9b-1541-9009-83952e7b672a", 
             "Driver": "MySQL", 
             "Config": {
                 "Gtid": "", 
                 "NatsAddr": "127.0.0.1:8193", 
                 "ParallelWorkers": 4,
                 "ReplChanBufferSize": 600, 
                 "MsgBytesLimit": 20480,
                 "ReplicateDoDb": [
                     {
                         "TableSchema": "sbtest"
                     }
                 ], 
                 "ConnectionConfig": {
                     "Host": "192.168.99.100", 
                     "Port": 13307,
                     "User": "root", 
                     "Password": "rootroot"
                 }
             }
         }, 
         {
             "Type": "Dest", 
             "NodeId": "a5e9ad80-8e77-03fd-7fa3-31e007ecdcbe", 
             "Driver": "MySQL", 
             "Config": {
                 "Gtid": "", 
                 "NatsAddr": "127.0.0.1:8193",  
                 "ParallelWorkers": 4, 
                 "ConnectionConfig": {
                     "Host": "192.168.99.100", 
                     "Port": 13309, 
                     "User": "root", 
                     "Password": "rootroot"
                 }
             }
         }
     ]
 }
 ````

输出
```` json
 {
     "Index": 7,
     "KnownLeader": false,
     "LastContact": 0,
     "Success": true
 }
 ````

### GET /jobs
## 1. 接口描述
该接口于查询数据同步/迁移作业列表，返回作业的详细信息。

## 2. 输入参数
无
## 3. 输出参数
返回一个数组对象，其中每一个元素为Object，其构成如下：

| 参数名称 | 类型 | 描述 |
|---------|---------|---------|
| ID | String |  |
| Name | String |  |
| JobSummary | Object | 返回的数据 |
| Status | Int | 数据任务执行状态，值包括：<br>running |
| Type | String | 数据任务类型，值包括：<br>synchronous-同步任务|