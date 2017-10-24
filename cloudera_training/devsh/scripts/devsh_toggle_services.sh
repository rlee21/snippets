#!/bin/sh

# This script conserves memory by enabling services specific to the
# Developer 1 course.



# Disable service: disable automatic startup, and make sure it is not running now
disable_service() {
	SERVICE=$1
    if [ -e /etc/init.d/$SERVICE ]; then 
        # disable automatic startup on boot
        sudo /sbin/chkconfig $SERVICE off
        
        # stop the service
        sudo /sbin/service $SERVICE stop
    fi
}

# Enable service: enable automatic startup, and make sure it is running now
enable_service() {
	SERVICE=$1
    if [ -e /etc/init.d/$SERVICE ]; then 
    
        # enable automatic startup on boot
        sudo /sbin/chkconfig $SERVICE on
        
        # avoid error by only starting service if it isn't already running
        sudo /sbin/service $SERVICE status >> /dev/null
        if [  $? -ne 0 ]; then sudo /sbin/service $SERVICE start; fi
    else
         echo "* Warning: Service $SERVICE not installed"
    fi
}


# Things we'll use in the class go here
echo "* Enabling and starting services required for DevSH Training"

enable_service hadoop-yarn-nodemanager 
enable_service hadoop-yarn-resourcemanager 
enable_service hadoop-hdfs-namenode
enable_service hadoop-hdfs-datanode
enable_service hadoop-mapreduce-historyserver 
enable_service hive-metastore
enable_service hive-server2
enable_service hue 
enable_service mysqld 
enable_service impala-state-store
enable_service impala-catalog
enable_service impala-server
enable_service spark-history-server
enable_service zookeeper-server 

# Things we won't use in the class go here
echo "* Disabling services not required for DevSH Training"

# disable other services
disable_service hadoop-hdfs-secondarynamenode
disable_service flume-ng-agent


