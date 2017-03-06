#!/bin/bash

function disable_systemd {
    systemctl disable udup
    rm -f /lib/systemd/system/udup.service
}

function disable_update_rcd {
    update-rc.d -f udup remove
    rm -f /etc/init.d/udup
}

function disable_chkconfig {
    chkconfig --del udup
    rm -f /etc/init.d/udup
}

if [[ "$1" == "0" ]]; then
    # RHEL and any distribution that follow RHEL, Amazon Linux covered
    # udup is no longer installed, remove from init system
    rm -f /etc/default/udup

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
    rm -f /etc/default/udup

    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
        disable_systemd
    else
        # Assuming sysv
        disable_update_rcd
    fi
fi
