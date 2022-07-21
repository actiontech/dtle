#!/bin/sh

sed -i "s/__HOSTNAME/$HOSTNAME/g" /dtle/etc/dtle/nomad.hcl

# Why use exec: https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
exec "$@"
