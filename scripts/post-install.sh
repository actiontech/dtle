#!/bin/bash

BIN_DIR=/usr/bin
LOG_DIR=/var/log/dtle
SCRIPT_DIR=/usr/lib/dtle/scripts
CONFIG_DIR=/etc/dtle
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    sed -i 's|'daemon=$BIN_DIR'|'daemon=$RPM_INSTALL_PREFIX$BIN_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh
    sed -i 's|'config=$CONFIG_DIR'|'config=$RPM_INSTALL_PREFIX$CONFIG_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh /etc/init.d/dtle
    chmod +x /etc/init.d/dtle
}

function install_systemd {
    sed -i 's|'ExecStart=$BIN_DIR'|'ExecStart=$RPM_INSTALL_PREFIX$BIN_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle.service
    sed -i 's|'-config\ $CONFIG_DIR'|'-config\ $RPM_INSTALL_PREFIX$CONFIG_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle.service
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle.service /lib/systemd/system/dtle.service
    systemctl enable dtle || true
    systemctl daemon-reload || true
}

function install_update_rcd {
    update-rc.d dtle defaults
}

function install_chkconfig {
    chkconfig --add dtle
}

id dtle &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd -r -K USERGROUPS_ENAB=yes -M dtle -s /bin/false -d /etc/dtle
fi
#CAP
# see `man capabilities`
#setcap CAP_DAC_OVERRIDE,CAP_SETUID,CAP_SYS_RESOURCE,CAP_SETGID=+eip $RPM_INSTALL_PREFIX$BIN_DIR/dtle
setcap CAP_DAC_OVERRIDE,CAP_SETUID,CAP_SETGID=+eip $RPM_INSTALL_PREFIX$BIN_DIR/dtle

test -d $LOG_DIR || mkdir -p $LOG_DIR
chown -R -L dtle:dtle $LOG_DIR
chmod 755 $LOG_DIR

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/dtle ]]; then
    rm -f /etc/init.d/dtle
fi
# Remove legacy symlink, if it exists
if [[ -L /etc/systemd/system/dtle.service ]]; then
    rm -f /etc/systemd/system/dtle.service
fi

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/dtle ]]; then
    touch /etc/default/dtle
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
	    systemctl restart dtle || echo "WARNING: systemd not running."
    else
	    # Assuming sysv
	    install_init
	    install_update_rcd
	    invoke-rc.d dtle restart
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
	    # Amazon Linux logic
	    install_init
	    install_chkconfig
    fi
fi
