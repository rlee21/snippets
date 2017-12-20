#!/bin/bash

# This script will advance the state of the VM as if the
# exercise specified on the command line (and all those 
# before it) had been completed. For example, invoking 
# this script as:
#
#   $ advance_exercises.sh exercise4
#
# Will prepare you to begin work on exercise 5, meaning 
# that the state of the VM will be exactly the same as if 
# exercise 4 (as well as exercises 3, 2, and 1) had been
# manually completed.
#
# BEWARE: In all invocations, this script will first run
#         a 'cleanup' step which removes data in HDFS and
#         the local filesystem in order to simulate the
#         original state of the VM. 
#

DEVSH=/home/training/training_materials/devsh
DEVDATA=/home/training/training_materials/data

# ensure we run any scripts from a writable local directory, 
# which needs to also not conflict with anything
RUNDIR=/tmp/devsh/exercisescripts/$RANDOM$$
mkdir -p $RUNDIR
cd $RUNDIR


cleanup() {
    echo "Cleaning up your system"

    sudo -u hdfs hdfs dfs -rm  -skipTrash -r -f /loudacre
    
    kafka-topics --if-exists --zookeeper localhost:2181 --delete --topic weblogs
}

exercise1() {
    echo "* Advancing through Exercise 1: Query Hadoop Data with Apache Impala"
    # Nothing required for subsequent exercises
}

exercise2() {
    # HDFS
    echo "* Advancing through Exercise 2: Access HDFS with the Command Line and Hue"
    
    hdfs dfs -mkdir /loudacre
    hdfs dfs -put $DEVDATA/kb /loudacre/
    hdfs dfs -put $DEVDATA/base_stations.tsv /loudacre/    
}

exercise3() {
    # YARN
    echo "* Advancing through Exercise 3: Run a YARN Job"

    # Nothing required for subsequent exercises

}

exercise4() {
    echo "* Advancing through Exercise 4: Import Data from MySQL Using Apache Sqoop"
 
    # Sqoop
    #  sqoop import \
    #   --connect jdbc:mysql://localhost/loudacre \
    #   --username training --password training \
    #   --table accounts \
    #   --target-dir /loudacre/accounts \
    #   --null-non-string '\\N'

    # sqoop import \
    # --connect jdbc:mysql://localhost/loudacre \
    # --username training --password training \
    # --table accounts \
    # --target-dir /loudacre/accounts_parquet \
    # --as-parquetfile

    
    # running the actual Sqoop import is too slow, 
    # so we use static data from a previous import.
    hdfs dfs -put $DEVDATA/static_data/accounts /loudacre/
    hdfs dfs -put $DEVDATA/static_data/accounts_parquet /loudacre/

}



exercise5() {
    echo "* Advancing through Exercise 5: Explore RDDs Using the Spark Shell"
    
    # Copy weblog data to HDFS /loudacre directory
    hdfs dfs -put $DEVDATA/weblogs/ /loudacre/
    
    # Copy in solution data
    # pyspark < $DEVSH/exercises/spark-shell/solution/SparkShell.pyspark
    hdfs dfs -put $DEVDATA/static_data/iplist /loudacre/
}


exercise6() {
    echo "* Advancing through Exercise 6: Process Data Files with Apache Spark"
    
    hdfs dfs -put $DEVDATA/activations /loudacre/  
    # pyspark <  $DEVSH/exercises/spark-etl/solution/ActivationModels.pyspark
    hdfs dfs -put $DEVDATA/static_data/account-models /loudacre/

    # Bonus exercise solution: Device Data ETL
    hdfs dfs -put $DEVDATA/devicestatus.txt /loudacre/ 
    # pyspark < $DEVSH/exercises/spark-etl/bonus/DeviceStatusETL.pyspark
    hdfs dfs -put $DEVDATA/static_data/devicestatus_etl /loudacre/
}


exercise7() {
    echo "* Advancing through Exercise 7: Use Pair RDDs to Join Two Datasets"
    # Exercise output not required for later exercises
}

exercise8() {
    echo "* Advancing through Exercise 8: Write and Run an Apache Spark Application "
    # Exercise output not required for later exercises
}


exercise9() {
    echo "* Advancing through Exercise 9: Configure an Apache Spark Application"
    # Exercise output not required for later exercises
}

exercise10() {
    echo "* Advancing through Exercise 10: View Jobs and Stages in the Spark Application UI"
    # Exercise output not required for later exercises
}

exercise11() {
    echo "* Advancing through Exercise 11: Persist an RDD"
    # Exercise output not required for later exercises
}

exercise12() {
    echo "* Advancing through Exercise 12: Implement an Iterative Algorithm with Apache Spark"
    # Exercise output not required for later exercises
}

exercise13() {
    echo "* Advancing through Exercise 13: Use Apache Spark SQL for ETL"
    # Exercise output not required for later exercises
}

exercise14() {
    # Kafka
    echo "* Advancing through Exercise 14: Produce and Consume Apache Kafka Messages"
    
    kafka-topics --create  --if-not-exists  --zookeeper localhost:2181  --replication-factor 1  --partitions 1   --topic weblogs

}

exercise15() {
    # Flume
    echo "* Advancing through Exercise 15: Collect Web Server Logs with Apache Flume"

    hdfs dfs -mkdir -p /loudacre/weblogs_flume
    hdfs dfs -put $DEVDATA/static_data/weblogs/* /loudacre/weblogs_flume/
    
    sudo mkdir -p /flume/weblogs_spooldir
    sudo chmod a+w -R /flume

}


exercise16() {
    # Flafka
    echo "* Advancing through Exercise 16: Send Web Server Log Messages from Apache Flume to Apache Kafka"
}

exercise17() {
    echo "* Advancing through Exercise 17: Write an Apache Spark Streaming Application"
    # Exercise output not required for later exercises
}

exercise18() {
    echo "* Advancing through Exercise 18: Process Multiple Batches with Apache Spark Streaming"
    # Exercise output not required for later exercises
}

exercise19() {
    echo "* Advancing through Exercise 19: Process Apache Kafka Messages with Apache Spark Streaming"
    # Exercise output not required for later exercises
}


case "$1" in
        
    cleanup)
        cleanup
        ;;

    exercise1)
        cleanup
        exercise1
        ;;

    exercise2)
        cleanup
        exercise1
        exercise2
        ;;

    exercise3)
        cleanup
        exercise1
        exercise2
        exercise3
        ;;

   exercise4)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        ;;

        
    exercise5)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        ;;

   exercise6)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        ;;

    exercise7)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        ;;

    exercise8)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
       ;;

    exercise9)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        ;;

    exercise10)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        ;;
        
    exercise11)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        ;;
        
    exercise12)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        ;;

    exercise13)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        ;;

    exercise14)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        ;;
        
    exercise15)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        ;;

        
    exercise16)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        ;;
                

        
    exercise17)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        exercise17
        ;;
          
    exercise18)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        exercise17
        exercise18
        ;;
                 
           
    exercise19)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        exercise17
        exercise18
        exercise19
        ;;
                
              
    *)
        echo $"Usage: $0 {exercise1-exercise19}"
        exit 1

esac