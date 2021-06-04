#!/bin/bash
SCRIPT_DIR=/usr/share/dtle/scripts
function install_systemd {
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service
    sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service

    if [[ -f /lib/systemd/system/dtle-consul.service ]];then
      cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service /lib/systemd/system/dtle-consul.service.rpmnew
    else
      cp $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-consul.service /lib/systemd/system/dtle-consul.service
    fi

    if [[ -f /lib/systemd/system/dtle-nomad.service ]];then
      cp -f $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service /lib/systemd/system/dtle-nomad.service.rpmnew
    else
      cp $RPM_INSTALL_PREFIX$SCRIPT_DIR/dtle-nomad.service /lib/systemd/system/dtle-nomad.service
    fi

    systemctl enable dtle-consul || true
    systemctl enable dtle-nomad || true
    systemctl daemon-reload || true
}

sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/dtle/*.hcl

systemctl --help &>/dev/null
if [[ $? -eq 0 ]]; then
  install_systemd

  if [[ -f /etc/debian_version ]]; then
    systemctl restart dtle-consul || echo "WARNING: failed to run systemctl start."
	  systemctl restart dtle-nomad || echo "WARNING: failed to run systemctl start."
  fi
else
  echo "No systemd. Please restart dtle-nomad manually."
fi