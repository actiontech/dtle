#!/bin/bash

if [[ -f /etc/opt/udup/udup.conf ]]; then
    # Legacy configuration found
    if [[ ! -d /etc/udup ]]; then
	# New configuration does not exist, move legacy configuration to new location
	echo -e "Please note, Udup's configuration is now located at '/etc/udup' (previously '/etc/opt/udup')."
	mv /etc/opt/udup /etc/udup

	backup_name="udup.conf.$(date +%s).backup"
	echo "A backup of your current configuration can be found at: /etc/udup/$backup_name"
	cp -a /etc/udup/udup.conf /etc/udup/$backup_name
    fi
fi
