# Udup REST API


<a name="overview"></a>
## Overview
You can communicate with Udup using a RESTful JSON API over HTTP. Udup nodes usually listen on port `8190` for API requests. All examples in this section assume that you've found a running leader at `localhost:8190`.

Udup implements a RESTful JSON API over HTTP to communicate with software clients. Udup listens in port `8190` by default. All examples in this section assume that you're using the default port.

Default API responses are unformatted JSON add the `pretty=true` param to format the response.


### Version information
*Version* : 0.1.0


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

<a name="status"></a>
### GET /

#### Description
Gets `Status` object.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[status](#status)|


#### Tags

* default


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
|**201**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="deletejob"></a>
### DELETE /jobs/{job_name}

#### Description
Delete a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**job_name**  <br>*required*|The job that needs to be deleted.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="showjobbyname"></a>
### GET /jobs/{job_name}

#### Description
Show a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**job_name**  <br>*required*|The job that needs to be fetched.|string||


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful response|[job](#job)|


#### Tags

* jobs


<a name="runjob"></a>
### POST /jobs/{job_name}

#### Description
Executes a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**job_name**  <br>*required*|The job that needs to be run.|string||


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
|**name**  <br>*required*|Name for the job.|string|
|**disabled**  <br>*optional*|Disabled state of the job|boolean|
|**parent_job**  <br>*optional*|The name/id of the job that will trigger the execution of this job  <br>*Example* : `"parent_job"`|string|
|**dependent_jobs**  <br>*optional*  <br>*read-only*|Array containing the jobs that depends on this one  <br>*Example* : `""`|string|
|**processors**  <br>*required*|Array containing the processors that will be called|< string, **DriverConfig** > map|

*Example*
``` json
{
    "name": "job1",
    "disabled": false,
    "parent_job": "",
    "dependent_jobs": [],
    "processors": {
        "extract": {
        	"driver": "mysql",
        	"server_id": 100,
        	"nats_addr": "127.0.0.1:13003",
            "conn_cfg": {
                "host": "192.168.99.100",
                "port": 13004,
                "user": "gm",
                "password": "111111"
            }
        },
        "apply": {
        	"driver": "mysql",
        	"nats_addr": "127.0.0.1:13003",
        	"nats_store_type": "MEMORY",
        	"nats_file_store_dir":"",
        	"worker_count": 1,
        	"batch": 1,
            "conn_cfg": {
                "host": "192.168.99.100",
                "port": 13005,
                "user": "gm",
                "password": "111111"
            }
        }
    },
    "concurrency": "allow"
}
```

<a name="DriverConfig"></a>
### DriverConfig
Arguments for calling an execution processor


|Name|Description|Schema|
|---|---|---|
|**driver**  <br>*required*|Type for the driver.|string|
|**server_id**  <br>*optional*|ServerID is the unique ID in mysql cluster|integer|
|**nats_addr**  <br>*optional*|address of the nats server|string|
|**nats_store_type**  <br>*optional*| Store type: MEMORY/FILE (default: MEMORY)|string|
|**nats_file_store_dir**  <br>*optional*| File to redirect message store|string|
|**worker_count**  <br>*optional*|Parallel worker count|integer|
|**batch**  <br>*optional*|Batch commit count|integer|
|**conn_cfg**  <br>*optional*|MySQL Configuration Properties|**ConnectionConfig**|

<a name="ConnectionConfig"></a>
### ConnectionConfig
Configuration properties define how connector will make a connection to a MySQL server


|Name|Description|Schema|
|---|---|---|
|**host**  <br>*required*|MySQL server host TCP connections|string|
|**user**  <br>*required*|MySQL server user TCP connections|string|
|**password**<br>*required*|MySQL server password TCP connections|string|
|**port**  <br>*required*|MySQL server port for TCP connections|integer|


<a name="member"></a>
### member
A member represents a cluster member node.


|Name|Description|Schema|
|---|---|---|
|**Name**  <br>*optional*|Node name|string|
|**Addr**  <br>*optional*|IP Address|string|
|**Port**  <br>*optional*|Port number|integer|
|**Tags**  <br>*optional*|Tags asociated with this node|< string, string > map|
|**Status**  <br>*optional*|The serf status of the node see: https://godoc.org/github.com/hashicorp/serf/serf#MemberStatus|integer|
|**ProtocolMin**  <br>*optional*|Serf protocol minimum version this node can understand or speak|integer|
|**ProtocolMax**  <br>*optional*||integer|
|**ProtocolCur**  <br>*optional*|Serf protocol current version this node can understand or speak|integer|
|**DelegateMin**  <br>*optional*|Serf delegate protocol minimum version this node can understand or speak|integer|
|**DelegateMax**  <br>*optional*|Serf delegate protocol minimum version this node can understand or speak|integer|
|**DelegateCur**  <br>*optional*|Serf delegate protocol minimum version this node can understand or speak|integer|