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


### Consumes

* `application/json`


### Produces

* `application/json`




<a name="paths"></a>
## Paths

<a name="getjobs"></a>
### GET /jobs

#### Description
List jobs.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|< [job](#job) > array|


#### Tags

* jobs


<a name="createorupdatejob"></a>
### POST /jobs

#### Description
Create or updates a new job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Body**|**body**  <br>*required*|Updated job object|[job](#job)||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="deletejob"></a>
### DELETE /job/{ID}

#### Description
Delete a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**ID**  <br>*required*|The job that needs to be deleted.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="showjobbyname"></a>
### GET /job/{ID}

#### Description
Show a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**ID**  <br>*required*|The job that needs to be fetched.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="runjob"></a>
### POST /job/{ID}/resume

#### Description
Executes a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**ID**  <br>*required*|The job that needs to be run.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs

<a name="stopjob"></a>
### POST /job/{ID}/pause

#### Description
Executes a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**ID**  <br>*required*|The job that needs to be stop.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="getleader"></a>
### GET /leader

#### Description
List members.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[member](#member)|


#### Tags

* default


<a name="leave"></a>
### GET /leave

#### Description
Force the node to leave the cluster.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|< [member](#member) > array|


#### Tags

* default


<a name="getmember"></a>
### GET /members

#### Description
List members.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|< [member](#member) > array|


#### Tags

* members




<a name="definitions"></a>
## Definitions

<a name="status"></a>
### status
Status represents details about the node.

*Type* : object


<a name="job"></a>
### job
A Job represents a scheduled task to execute.


|Name|Description|Schema|
|---|---|---|
|**Name**  <br>*required*|Name for the job.|string|
|**Type**  <br>*required*|job type (synchronous)|boolean|
|**Tasks**  <br>*required*|Array containing the tasks that will be called|< string, **Task** > array|

<a name="Task"></a>
### Task
Arguments for calling an execution task


|Name|Description|Schema|
|---|---|---|
|**Type**  <br>*required*|Type for the task.|string|
|**NodeId**  <br>*optional*|Parallel worker count|string|
|**Driver**  <br>*required*|Driver for the task.|string|
|**Config**  <br>*required*|DataSource Configuration Properties|**Config**|

<a name="Config"></a>
### Config
Configuration properties define how connector will make a connection to a MySQL server


|Name|Description|Schema|
|---|---|---|
|**Gtid**  <br>*optional*|MySQL Gtid|string|
|**NatsAddr**  <br>*required*|Nats host|string|
|**ParallelWorkers**<br>*optional*|Parallel worker count|integer|
|**ReplChanBufferSize**  <br>*optional*|buffer size|integer|
|**MsgBytesLimit**  <br>*optional*|msg limit|integer|
|**ReplicateDoDb**  <br>*optional*|DataSource Configuration Properties|**ReplicateDoDb**|
|**ConnectionConfig**  <br>*required*|DataSource Configuration Properties|**ConnectionConfig**|

<a name="ReplicateDoDb"></a>
### ReplicateDoDb
Configuration properties define how connector will make a connection to a MySQL server


|Name|Description|Schema|
|---|---|---|
|**TableSchema**  <br>*required*|MySQL server host TCP connections|string|
|**Tables**  <br>*required*|MySQL server user TCP connections|string|

<a name="ConnectionConfig"></a>
### ConnectionConfig
Configuration properties define how connector will make a connection to a MySQL server


|Name|Description|Schema|
|---|---|---|
|**host**  <br>*required*|MySQL server host TCP connections|string|
|**user**  <br>*required*|MySQL server user TCP connections|string|
|**password**<br>*required*|MySQL server password TCP connections|string|
|**port**  <br>*required*|MySQL server port for TCP connections|integer|


*Example*
``` json
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
                    "Key": {
                        "Host": "192.168.99.100", 
                        "Port": 13307
                    }, 
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
                    "Key": {
                        "Host": "192.168.99.100", 
                        "Port": 13309
                    }, 
                    "User": "root", 
                    "Password": "rootroot"
                }
            }
        }
    ]
}
```