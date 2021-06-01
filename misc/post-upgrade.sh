#!/bin/bash

sed -i 's|INSTALL_PREFIX_MAGIC|'$RPM_INSTALL_PREFIX'|g' $RPM_INSTALL_PREFIX/etc/dtle/*.hcl

