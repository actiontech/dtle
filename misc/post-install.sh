#!/bin/bash

SCRIPT_DIR=/usr/share/dtle/scripts

function install_systemd {
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service /lib/systemd/system/
    cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service /lib/systemd/system/
    systemctl enable dtle-consul || true
    systemctl enable dtle-nomad || true
    systemctl daemon-reload || true
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

sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/dtle/*.hcl

which systemctl &>/dev/null
if [[ $? -eq 0 ]]; then
  install_systemd

  if [[ -f /etc/debian_version ]]; then
    systemctl restart dtle-consul || echo "WARNING: failed to run systemctl start."
	  systemctl restart dtle-nomad || echo "WARNING: failed to run systemctl start."
  fi
else
  echo "No systemd. Please start/stop dtle-nomad manually."
fi

