#!/bin/sh

wget https://raw.githubusercontent.com/jhuckaby/Cronicle/master/bin/install.js
cd ~
node install.js
chown dataeng /opt/cronicle
/opt/cronicle/bin/control.sh setup
/opt/cronicle/bin/control.sh start