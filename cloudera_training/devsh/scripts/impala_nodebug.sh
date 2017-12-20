#!/bin/sh
# Switch Impala to use regular instead of debug binaries
# This is a workaround for https://issues.cloudera.org/browse/IMPALA-1646

# Stop all Impala services
echo "Stopping Impala services"
sudo service impala-server stop
sudo service impala-catalog stop
sudo service impala-state-store stop
sudo service hive-metastore stop

# Use the non-debug version of the course impala defaults script

if [ -L "/etc/default/impala" ]; then
    sudo rm /etc/default/impala
    sudo ln -s /etc/default/impala.training /etc/default/impala
else
    echo "Error: /etc/default/impala is not a link, leaving"
    exit 1
fi
 
# Restart Impala services   
echo "Restarting Impala services"
sudo service hive-metastore start
sleep 10
sudo service impala-state-store start
sudo service impala-catalog start
sudo service impala-server start
sleep 10

echo "Impala now running in non-debug mode"