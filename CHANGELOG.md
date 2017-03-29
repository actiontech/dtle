## v0.2.0 [2017-03-29]

### Features 
- Support service health queries and failover to remote nodes based on network coordinates
- Allows filtering production of DML events by GTIDs
- Populate job status
- Improved restart policy

### Bugfixes

## v0.1.3 [2017-03-09]

### Features 
- MySQL connector supports failing over based on subset of GTIDs
- Use docker images with the custom scripts and config baked into the image
- Enhance Udup Consul image to support clustering

### Bugfixes

## v0.1.0 [2017-03-03]

### Features 
- Ingest change data from MySQL databases
- Processor plugins, allows flexible routing of execution results
- Ability to encrypt serf network traffic between nodes.
- Using Libkv allows to use consul storage backends
- Includes cluster nodes
- Compiled with Go 1.7

### Bugfixes

- [#UDUP-2](http://10.186.18.21/universe/udup/issues/2): 源库有修改密码的话启动时会报错.
- [#UDUP-1](http://10.186.18.21/universe/udup/issues/1): Add support for MySQL GTIDs.

This is the initial release of Udup.
