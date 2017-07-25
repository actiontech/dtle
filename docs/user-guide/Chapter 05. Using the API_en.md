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
## 1. API Description
This API is used to create data synchronization/migration task and return the result of data synchronization task.

## 2. Input Parameters
The following request parameter list only provides API request parameters.

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| ID | No | Int | ID of data synchronization job. Please use API "Query Data Synchronization Task List" to query the task ID |
| Name | Yes | String | Name of data replication task |
| Type | Yes | String | Type of data replication task. Possible values include: < br> synchronous <br> migration <br> subscribe |
| Tasks | Yes | Array |  |

Each element in the Tasks is an Object, which is composed of the following parameters:

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| Type | Yes | String | task type（extract/apply）,Possible values include: <br>Src-source<br>Dest-dest |
| Driver | Yes | String | task driver,Possible values include: <br>MySQL<br>Oracle |
| NodeId | No | String | node id |
| Config | Yes | Object | config |

Parameter Config is composed of the following parameters:

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| Gtid | No | String | MySQL Gtid |
| NatsAddr | Yes | String | nats server address |
| ConnectionConfig | Yes | Object | MySQL connection |
| ReplicateDoDb | No | Array | MySQL Gtid |

Parameter ConnectionConfig is composed of the following parameters:

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| Host | Yes | String | MySQL server host TCP connections |
| Port | Yes | Int | MySQL server port for TCP connections |
| User | Yes | String | MySQL server user TCP connections |
| Password | Yes | String | MySQL server password TCP connections |

Parameter ReplicateDoDb is composed of the following parameters:

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| TableSchema | No | String | table schema
| Tables | No | Array | tables name

Parameter Tables is composed of the following parameters:

| Parameter Name | Required | Type | Description |
|---------|---------|---------|---------|
| TableName | No | String | table name

## 3. Output Parameters
| Parameter Name | Type | Description |
|---------|---------|---------|
| Success | Bool | Module error message description depending on API.  |

## 4. Example
Input
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

Output
```` json
 {
     "Index": 7,
     "KnownLeader": false,
     "LastContact": 0,
     "Success": true
 }
 ```


