#!/bin/bash

function disable_systemd {
    systemctl disable dts
    rm -f /lib/systemd/system/dts.service
}

function disable_update_rcd {
    update-rc.d -f dts remove
    rm -f /etc/init.d/dts
}

function disable_chkconfig {
    chkconfig --del dts
    rm -f /etc/init.d/dts
}

if [[ "$1" == "0" ]]; then
    # RHEL and any distribution that follow RHEL, Amazon Linux covered
    # dts is no longer installed, remove from init system
    rm -f /etc/default/dts

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
    rm -f /etc/default/dts

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
        disable_systemd
    else
        # Assuming sysv
        disable_update_rcd
    fi
fi
