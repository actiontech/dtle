#!/bin/bash

function disable_systemd {
    systemctl stop dtle-consul
    systemctl stop dtle-nomad
    systemctl disable dtle-consul
    systemctl disable dtle-nomad
    rm -f /lib/systemd/system/dtle-consul.service
    rm -f /lib/systemd/system/dtle-nomad.service
}

function disable_update_rcd {
    update-rc.d -f dtle remove
    rm -f /etc/init.d/dtle
}

function disable_chkconfig {
    chkconfig --del dtle
    rm -f /etc/init.d/dtle
}

if [[ "$1" == "0" ]]; then
    # RHEL and any distribution that follow RHEL, Amazon Linux covered
    # dtle is no longer installed, remove from init system

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
        disable_systemd
    else
        # Assuming sysv
        disable_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    # Remove/purge

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
      	deb-systemd-invoke stop dtle-nomad.service
	      deb-systemd-invoke stop dtle-consul.service
        disable_systemd
    else
        # Assuming sysv
      	invoke-rc.d dtle stop
        disable_update_rcd
    fi
fi
