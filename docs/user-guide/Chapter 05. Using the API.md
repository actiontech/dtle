# Udup REST API


<a name="overview"></a>
## Overview
You can communicate with Udup using a RESTful JSON API over HTTP. Udup nodes usually listen on port `8190` for API requests. All examples in this section assume that you've found a running leader at `localhost:8190`.

Udup implements a RESTful JSON API over HTTP to communicate with software clients. Udup listens in port `8190` by default. All examples in this section assume that you're using the default port.

Default API responses are unformatted JSON add the `pretty=true` param to format the response.


### Version information
*Version* : 0.3.0


### URI scheme
*Host* : localhost:8190  
*BasePath* : /v1  
*Schemes* : HTTP

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
| Type | 是 | String | 数据复制任务类型（抽取/回放）,可取值包括：<br>Src-源端<br>Dest-目标端 |
| Driver | 是 | String | 数据复制对象类型,可取值包括：<br>MySQL<br>Oracle |
| NodeId | 否 | String | 抽取节点ID，可使用[查询节点列表](/docs/api/) 接口获取，其值为输出参数中字段 id 的值。 |
| Config | 是 | Object | 配置信息 |

Config 为该任务中数据相关的配置，字段描述为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| Gtid | 否 | String | MySQL Gtid |
| NatsAddr | 是 | String | nats server address |
| ConnectionConfig | 是 | Object | MySQL connection |
| ReplicateDoDb | 否 | Array | MySQL Gtid |

其中， ConnectionConfig 的构成为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| Host | 是 | String | MySQL server host TCP connections |
| Port | 是 | Int | MySQL server port for TCP connections |
| User | 是 | String | MySQL server user TCP connections |
| Password | 是 | String | MySQL server password TCP connections |

其中， ReplicateDoDb 的构成为：

| 参数名称 | 是否必选  | 类型 | 描述 |
|---------|---------|---------|---------|
| TableSchema | 否 | String | 数据复制schema对象
| Tables | 否 | Array | 数据复制table对象集合

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
 ```

