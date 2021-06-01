#!/bin/bash

which systemctl &>/dev/null
if [[ $? -eq 0 ]]; then
    systemctl stop dtle-consul
    systemctl stop dtle-nomad
else
    echo "No systemd. Please stop dtle-nomad manually before upgrade."
fi