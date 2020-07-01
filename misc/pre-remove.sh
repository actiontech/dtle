#!/bin/bash

function disable_systemd {
    systemctl stop dtle-consul
    systemctl stop dtle-nomad
    systemctl disable dtle-consul
    systemctl disable dtle-nomad
    rm -f /lib/systemd/system/dtle-consul.service
    rm -f /lib/systemd/system/dtle-nomad.service
}

which systemctl &>/dev/null
if [[ $? -eq 0 ]]; then
    if [[ -f /etc/debian_version ]]; then
      # Debian/Ubuntu logic
      # Remove/purge
        deb-systemd-invoke stop dtle-nomad.service
	      deb-systemd-invoke stop dtle-consul.service
    fi

    disable_systemd
else
    # Assuming sysv
    disable_chkconfig
fi
