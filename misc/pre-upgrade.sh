#!/bin/bash

which systemctl &>/dev/null
if [[ $? -eq 0 ]]; then
    systemctl stop dtle-consul
    systemctl stop dtle-nomad
fi