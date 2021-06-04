#!/bin/bash

sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/dtle/*.hcl

systemctl --help &>/dev/null
if [[ $? -eq 0 ]]; then
  if [[ -f /etc/debian_version ]]; then
    systemctl restart dtle-consul || echo "WARNING: failed to run systemctl start."
	  systemctl restart dtle-nomad || echo "WARNING: failed to run systemctl start."
  fi
else
  echo "No systemd. Please restart dtle-nomad manually."
fi