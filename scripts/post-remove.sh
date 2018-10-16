#!/bin/bash

function disable_systemd {
    systemctl disable dtle
    rm -f /lib/systemd/system/dtle.service
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
    rm -f /etc/default/dtle

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
        disable_systemd
    else
        # Assuming sysv
        disable_chkconfig
    fi
elif [ "$1" == "remove" -o "$1" == "purge" ]; then
    # Debian/Ubuntu logic
    # Remove/purge
    rm -f /etc/default/dtle

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
        disable_systemd
    else
        # Assuming sysv
        disable_update_rcd
    fi
fi
