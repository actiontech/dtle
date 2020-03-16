#!/bin/bash

BIN_DIR=/usr/bin
LOG_DIR=/var/log/dts
SCRIPT_DIR=/usr/lib/dts/scripts
CONFIG_DIR=/etc/dts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    sed -i 's|'daemon=$BIN_DIR'|'daemon=$RPM_INSTALL_PREFIX$BIN_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh
    sed -i 's|'config=$CONFIG_DIR'|'config=$RPM_INSTALL_PREFIX$CONFIG_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/init.sh /etc/init.d/dts
    chmod +x /etc/init.d/dts
}

function install_systemd {
    sed -i 's|'ExecStart=$BIN_DIR'|'ExecStart=$RPM_INSTALL_PREFIX$BIN_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dts.service
    sed -i 's|'-config\ $CONFIG_DIR'|'-config\ $RPM_INSTALL_PREFIX$CONFIG_DIR'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dts.service
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dts.service /lib/systemd/system/dts.service
    systemctl enable dts || true
    systemctl daemon-reload || true
}

function install_update_rcd {
    update-rc.d dts defaults
}

function install_chkconfig {
    chkconfig --add dts
}

id dts &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd -r -K USERGROUPS_ENAB=yes -M dts -s /bin/false -d /etc/dts
fi
#CAP
# see `man capabilities`
#setcap CAP_DAC_OVERRIDE,CAP_SETUID,CAP_SYS_RESOURCE,CAP_SETGID=+eip $RPM_INSTALL_PREFIX$BIN_DIR/dts
setcap CAP_DAC_OVERRIDE,CAP_SETUID,CAP_SETGID=+eip $RPM_INSTALL_PREFIX$BIN_DIR/dts

test -d $LOG_DIR || mkdir -p $LOG_DIR
chown -R -L dts:dts $LOG_DIR
chmod 755 $LOG_DIR

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/dts ]]; then
    rm -f /etc/init.d/dts
fi
# Remove legacy symlink, if it exists
if [[ -L /etc/systemd/system/dts.service ]]; then
    rm -f /etc/systemd/system/dts.service
fi

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/dts ]]; then
    touch /etc/default/dts
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
	    systemctl restart dts || echo "WARNING: systemd not running."
    else
	    # Assuming sysv
	    install_init
	    install_update_rcd
	    invoke-rc.d dts restart
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
	    # Amazon Linux logic
	    install_init
	    install_chkconfig
    fi
fi
