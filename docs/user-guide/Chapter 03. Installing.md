Chapter 3. Installing
===================

This guide will get you up and running with Udup. It walks you through the package, installation, and configuration processes, and it shows how to use Udup to extract and apply data with mysql.

##3.1 Package and Install Udup

which gets installed via the Makefile
if you don't have it already. You also must build with golang version 1.5+.

1. [Install Go](https://golang.org/doc/install)
2. [Setup your GOPATH](https://golang.org/doc/code.html#GOPATH)
3. Run `git clone git@10.186.18.21:universe/udup.git`
4. Run `cd $GOPATH/src/udup`
5. Run `make prepare package`

Note: Udup will start automatically using the default configuration when installed from a deb or rpm package.

##3.2 Configuration

Configuration file location by installation type
- Linux debian and RPM packages: /etc/udup/udup.conf
- Standalone Binary: see the [configuration guide](./Chapter%2004.%20Configuration.md) for how to create a configuration file

##3.3 Start the Udup Server

- Linux (sysvinit and upstart installations)
sudo service udup start
- Linux (systemd installations)
systemctl start udup

##3.4 Running Udup with Docker

- **Start Udup**
Open a new terminal, and use it to start the Udup service in a new container by running:

 > $ docker run -it --rm --name udup -p 8190:8190 -e GROUP_ID=1 --link udup-consul:udup-consul actiontech/udup-runtime-centos7

This runs a new Docker container named udup using the actiontech/udup image.
You should see in your terminal the typical output of Udup, ending with:

 > ...
2017-03-09 14:48:16,223 INFO   ||  Udup version : 0.1.3  

- **Start a MySQL database**
At this point, we’ve started Consul and Udup, but we don’t yet have a database server from which Udup can capture changes. Now, let’s start a MySQL server with an example database.
Open a new terminal, and use it to start a new container that runs a MySQL database server preconfigured with an inventory database:
  > $ docker run -it --rm --name udup-mysql -p 13306:3306 -e MYSQL_ROOT_PASSWORD=rootroot -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpwd actiontech/udup-mysql
                                
  This runs a new container using version v1 of the debezium/example-mysql image, which is based on the mysql:5.7 image, defines and populate a sample "inventory" database, and creates a udup user with password 'mysqlpwd' that has the minimum privileges required by Udup’s MySQL connector. The command assigns the name mysql to the container so that it can be easily referenced later. The -it flag makes the container interactive, meaning it attaches the terminal’s standard input and output to the container so that you can see what is going on in the container. The --rm flag instructs Docker to remove the container when it is stopped. The command maps port 3306 (the default MySQL port) in the container to the same port on the Docker host so that software outside of the container can connect to the database server. And finally, it also uses the -e option three times to set the MYSQL_ROOT_PASSWORD, MYSQL_USER, and MYSQL_PASSWORD environment variables to specific values.
  You should see in your terminal something like the following:
                                  
 > ...
    2017-03-07T02:30:24.963832Z 0 [Note] mysqld: ready for connections.
    Version: '5.7.17-log'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server (GPL)
                                     
 Notice that the MySQL server starts and stops a few times as the configuration is modified. The last line listed above reports that the MySQL server is running and ready for use.    

##3.5 Using  REST API
The Udup service exposes a RESTful API to manage the set of connectors, so let’s use that API using the curl command line tool. Because we mapped port 8190 in the connect container (where the Udup service is running) to port 8190 on the Docker host, we can communicate to the service by sending the request to port 8190 on the Docker host, which then forwards the request to the Udup service.
Open a new terminal, and use it to check the status of the Udup service:

> $ curl -H "Accept:application/json" localhost:8190/

Please see the [api guide](./Chapter%2005.%20Using%20the%20API_en.md) for details of the API services.