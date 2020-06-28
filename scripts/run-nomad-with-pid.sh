#!/bin/sh

nohup INSTALL_PREFIX_MAGIC/usr/bin/nomad agent -config INSTALL_PREFIX_MAGIC/etc/nomad/single.hcl >>/dev/null 2>>INSTALL_PREFIX_MAGIC/var/log/nomad/nomad.log &
echo $! > INSTALL_PREFIX_MAGIC/var/run/nomad/nomad.pid
