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
### POST /jobs/{job_name}/start

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

<a name="stopjob"></a>
### POST /jobs/{job_name}/stop

#### Description
Executes a job.


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**job_name**  <br>*required*|The job that needs to be stop.|string||


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
|**status**  <br>*optional*|Enabled state of the job|boolean|
|**processors**  <br>*required*|Array containing the processors that will be called|< string, **DriverConfig** > map|

*Example*
``` json
{
    "name": "job1",
    "failover": false,
    "processors": {
        "src": {
            "node_name": "node2",
            "replicate_do_db": [
                {
                    "db_name": "s1",
                    "tb_name": "dbtest"
                }
            ],
            "driver": "mysql",
            "conn_cfg": {
                "host": "192.168.99.100",
                "port": 13307,
                "user": "root",
                "password": "rootroot"
            }
        },
        "dest": {
            "node_name": "node1",
            "driver": "mysql",
            "gtid": "",
            "worker_count": 1,
            "conn_cfg": {
                "host": "192.168.99.100",
                "port": 13308,
                "user": "root",
                "password": "rootroot"
            }
        }
    }
}
```

<a name="DriverConfig"></a>
### DriverConfig
Arguments for calling an execution processor


|Name|Description|Schema|
|---|---|---|
|**driver**  <br>*required*|Type for the driver.|string|
|**worker_count**  <br>*optional*|Parallel worker count|integer|
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