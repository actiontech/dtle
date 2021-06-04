#!/bin/bash

which systemctl &>/dev/null
if [[ $? -eq 0 ]]; then
    if [[ -f /lib/systemd/system/dtle-consul.service ]];then
      systemctl stop dtle-consul
    fi

    if [[ -f /lib/systemd/system/dtle-consul.service ]];then
      systemctl stop dtle-nomad
    fi
fi