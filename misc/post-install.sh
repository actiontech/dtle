#!/bin/bash

SCRIPT_DIR=/usr/share/dtle/scripts

function install_init {
    echo "TODO"; false
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.sh
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.sh
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.sh /etc/init.d/dtle-nomad
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.sh /etc/init.d/dtle-consul
    chmod +x /etc/init.d/dtle-nomad
    chmod +x /etc/init.d/dtle-consul
}

function install_systemd {
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service /lib/systemd/system/
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service /lib/systemd/system/
    systemctl enable dtle-consul || true
    systemctl enable dtle-nomad || true
    systemctl daemon-reload || true
}

function install_update_rcd {
    update-rc.d dtle-consul defaults
    update-rc.d dtle-nomad defaults
}

function install_chkconfig {
    chkconfig --add dtle-consul
    chkconfig --add dtle-nomad
}

id dtle &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd -r -K USERGROUPS_ENAB=yes -M dtle -s /bin/false -d /etc/dtle
fi
#CAP
# see `man capabilities`
#setcap CAP_DAC_OVERRIDE,CAP_SETUID,CAP_SETGID=+eip $RPM_INSTALL_PREFIX/usr/bin/dtle

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/dtle ]]; then
    rm -f /etc/init.d/dtle
fi
# Remove legacy symlink, if it exists
if [[ -L /etc/systemd/system/dtle.service ]]; then
    rm -f /etc/systemd/system/dtle.service
fi

mkdir -p "$RPM_INSTALL_PREFIX/var/lib/consul"
mkdir -p "$RPM_INSTALL_PREFIX/var/log/consul"
mkdir -p "$RPM_INSTALL_PREFIX/var/lib/nomad"
mkdir -p "$RPM_INSTALL_PREFIX/var/log/nomad"

chown -R -L dtle:dtle "$RPM_INSTALL_PREFIX/var/lib/consul"
chown -R -L dtle:dtle "$RPM_INSTALL_PREFIX/var/log/consul"
chown -R -L dtle:dtle "$RPM_INSTALL_PREFIX/var/lib/nomad"
chown -R -L dtle:dtle "$RPM_INSTALL_PREFIX/var/log/nomad"

sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/consul/*.hcl
sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/nomad/*.hcl
sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/run-nomad-with-pid.sh

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
	    systemctl restart dtle-consul || echo "WARNING: failed to run systemctl start."
	    systemctl restart dtle-nomad || echo "WARNING: failed to run systemctl start."
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
