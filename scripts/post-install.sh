#!/bin/bash

BIN_DIR=/usr/bin
LOG_DIR=/var/log/udup
SCRIPT_DIR=/usr/lib/udup/scripts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/udup
    chmod +x /etc/init.d/udup
}

function install_systemd {
    cp -f $SCRIPT_DIR/udup.service /lib/systemd/system/udup.service
    systemctl enable udup || true
    systemctl daemon-reload || true
}

function install_update_rcd {
    update-rc.d udup defaults
}

function install_chkconfig {
    chkconfig --add udup
}

id udup &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd -r -K USERGROUPS_ENAB=yes -M udup -s /bin/false -d /etc/udup
fi

test -d $LOG_DIR || mkdir -p $LOG_DIR
chown -R -L udup:udup $LOG_DIR
chmod 755 $LOG_DIR

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/udup ]]; then
    rm -f /etc/init.d/udup
fi
# Remove legacy symlink, if it exists
if [[ -L /etc/systemd/system/udup.service ]]; then
    rm -f /etc/systemd/system/udup.service
fi

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/udup ]]; then
    touch /etc/default/udup
fi

# Add .d configuration directory
if [[ ! -d /etc/udup/udup.d ]]; then
    mkdir -p /etc/udup/udup.d
fi

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
	    install_systemd
    else
	    # Assuming sysv
	    install_init
	    install_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
	    install_systemd
	    systemctl restart udup || echo "WARNING: systemd not running."
    else
	    # Assuming sysv
	    install_init
	    install_update_rcd
	    invoke-rc.d udup restart
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
	    # Amazon Linux logic
	    install_init
	    install_chkconfig
    fi
fi
