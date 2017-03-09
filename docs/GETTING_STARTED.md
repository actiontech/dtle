# Getting started with Udup

This guide will get you up and running with Udup. It walks you through the package, installation, and configuration processes, and it shows how to use Udup to extract and apply data with mysql.

## Package and Install Udup

which gets installed via the Makefile
if you don't have it already. You also must build with golang version 1.5+.

1. [Install Go](https://golang.org/doc/install)
2. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
3. Run `git clone git@10.186.18.21:universe/udup.git`
4. Run `cd $GOPATH/src/udup`
5. Run `make package`

Note: Udup will start automatically using the default configuration when installed from a deb or rpm package.

## Configuration

Configuration file location by installation type
- Linux debian and RPM packages: /etc/udup/udup.conf
- Standalone Binary: see the [configuration guide](./docs/CONFIGURATION.md) for how to create a configuration file

## Start the Udup Server

- Linux (sysvinit and upstart installations)
sudo service udup start
- Linux (systemd installations)
systemctl start udup

## Running Udup with Docker

Running Udup involves two major services: Consul and Udup service. This tutorial walks you through starting a single instance of these services using Docker and Udup’s Docker images. Production environments, on the other hand, require running multiple instances of each service to provide the performance, reliability, replication, and fault tolerance. 

- **Start Consul**
Start a new terminal and start a container with Consul by running:
 > $ docker run -it --rm --name udup-consul -p 8500:8500 consul
 
 This runs a new container using version 0.7.5 of the consul image, and assigns the name udup-consul to this container. The -it flag makes the container interactive, meaning it attaches the terminal’s standard input and output to the container so that you can see what is going on in the container. The --rm flag instructs Docker to remove the container when it is stopped. The three -p options map three of the container’s ports (e.g., 2181, 2888, and 3888) to the same ports on the Docker host so that other containers (and software outside the container) can talk with Consul.
You should see in your terminal the typical output of Consul:
 
 > ==> Starting Consul agent...
   ==> Starting Consul agent RPC...
   ==> Consul agent running!
	        Version: 'v0.7.5'
	        Node name: '300056c02bd8'
	        Datacenter: 'dc1'
	        Server: true (bootstrap: false)
	        Client Addr: 0.0.0.0 (HTTP: 8500, HTTPS: -1, DNS: 8600, RPC: 8400)
     
     
 The last line is important and reports that Consul is ready and listening on 		port 8500. The terminal will continue to show additional output as Consul  generates it.

- **Start Udup**
Open a new terminal, and use it to start the Udup service in a new container by running:

 > $ docker run -it --rm --name udup -p 8190:8190 -e GROUP_ID=1 --link udup-consul:udup-consul actiontech/udup-runtime-centos7

 This runs a new Docker container named connect using version 0.4 of the debezium/connect image. The -it flag makes the container interactive, meaning it attaches the terminal’s standard input and output to the container so that you can see what is going on in the container. The --rm flag instructs Docker to remove the container when it is stopped. The command maps port 8083 in the container to the same port on the Docker host so that software outside of the container can use Kafka Connect’s REST API to set up and manage new connector instances. The command uses the --link zookeeper:zookeeper, --link kafka:kafka, and --link mysql:mysql, arguments to tell the container that it can find Zookeeper running in the container named zookeeper, the Kafka broker running in the container named kafka, and the MySQL server running in the container named mysql, all running on the same Docker host. And finally, it also uses the -e option three times to set the GROUP_ID, CONFIG_STORAGE_TOPIC, and OFFSET_STORAGE_TOPIC environment variables, which are all required by this Debezium image (though you can use different values as desired).
You should see in your terminal the typical output of Kafka, ending with:

 > ...
2017-02-07 20:48:16,223 INFO   ||  Udup version : 0.10.1.1  

- **Start a MySQL database**
At this point, we’ve started Consul and Udup, but we don’t yet have a database server from which Udup can capture changes. Now, let’s start a MySQL server with an example database.
Open a new terminal, and use it to start a new container that runs a MySQL database server preconfigured with an inventory database:
  > $ docker run -it --rm --name udup-mysql -p 13306:3306 -e MYSQL_ROOT_PASSWORD=rootroot -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpwd actiontech/udup-mysql
                                
  This runs a new container using version v1 of the debezium/example-mysql image, which is based on the mysql:5.7 image, defines and populate a sample "inventory" database, and creates a udup user with password '111111' that has the minimum privileges required by Udup’s MySQL connector. The command assigns the name mysql to the container so that it can be easily referenced later. The -it flag makes the container interactive, meaning it attaches the terminal’s standard input and output to the container so that you can see what is going on in the container. The --rm flag instructs Docker to remove the container when it is stopped. The command maps port 3306 (the default MySQL port) in the container to the same port on the Docker host so that software outside of the container can connect to the database server. And finally, it also uses the -e option three times to set the MYSQL_ROOT_PASSWORD, MYSQL_USER, and MYSQL_PASSWORD environment variables to specific values.
  You should see in your terminal something like the following:
                                  
 > ...
    2017-03-07T02:30:24.963832Z 0 [Note] mysqld: ready for connections.
    Version: '5.7.17-log'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server (GPL)
                                     
 Notice that the MySQL server starts and stops a few times as the configuration is modified. The last line listed above reports that the MySQL server is running and ready for use.    

##Using  REST API
The Udup service exposes a RESTful API to manage the set of connectors, so let’s use that API using the curl command line tool. Because we mapped port 8190 in the connect container (where the Udup service is running) to port 8190 on the Docker host, we can communicate to the service by sending the request to port 8190 on the Docker host, which then forwards the request to the Udup service.
Open a new terminal, and use it to check the status of the Udup service:

> $ curl -H "Accept:application/json" localhost:8190/

Please see the [api guide](./doc/API.md) for details of the API services.                                                           