#!/bin/bash

DEVSH=/home/training/training_materials/devsh

# Numbers here 0 based, in adv script 1 based (on purpose)
setExerciseNames() {
  EXERCISES[0]="Query Hadoop Data with Apache Impala"
  EXERCISES[1]="Access HDFS with the Command Line and Hue"
  EXERCISES[2]="Run a YARN Job"
  EXERCISES[3]="Import Data from MySQL Using Apache Sqoop"
  EXERCISES[4]="Explore RDDs Using the Spark Shell"
  EXERCISES[5]="Process Data Files with Apache Spark"
  EXERCISES[6]="Use Pair RDDs to Join Two Datasets"
  EXERCISES[7]="Write and Run an Apache Spark Application"
  EXERCISES[8]="Configure an Apache Spark Application"
  EXERCISES[9]="View Jobs and Stages in the Spark Application UI"
  EXERCISES[10]="Persist an RDD"
  EXERCISES[11]="Implement an Iterative Algorithm with Apache Spark"
  EXERCISES[12]="Use Apache Spark SQL for ETL"
  EXERCISES[13]="Produce and Consume Apache Kafka Messages"
  EXERCISES[14]="Collect Web Server Logs with Apache Flume"
  EXERCISES[15]="Send Web Server Log Messages from Apache Flume to Apache Kafka"
  EXERCISES[16]="Write an Apache Spark Streaming Application"
  EXERCISES[17]="Process Multiple Batches with Apache Spark Streaming"
  EXERCISES[18]="Process Apache Kafka Messages with Apache Spark Streaming"
}

getStartState() {
  validResponse=0
  while [ $validResponse -eq 0 ] 
  do 
    echo ""
    echo "Please enter the number of the exercise that you want to do."
    echo "This script will reset your system to the start state for that exercise."
    echo ""
    echo " 1" ${EXERCISES[0]}
    echo " 2" ${EXERCISES[1]} 
    echo " 3" ${EXERCISES[2]} 
    echo " 4" ${EXERCISES[3]}
    echo " 5" ${EXERCISES[4]}
    echo " 6" ${EXERCISES[5]}
    echo " 7" ${EXERCISES[6]}
    echo " 8" ${EXERCISES[7]}
    echo " 9" ${EXERCISES[8]}
    echo "10" ${EXERCISES[9]}
    echo "11" ${EXERCISES[10]}
    echo "12" ${EXERCISES[11]}
    echo "13" ${EXERCISES[12]}
    echo "14" ${EXERCISES[13]}
    echo "15" ${EXERCISES[14]}
    echo "16" ${EXERCISES[15]}
    echo "17" ${EXERCISES[16]}
    echo "18" ${EXERCISES[17]}
    echo "19" ${EXERCISES[18]}
    echo ""
    read EXERCISE
    if [[ $EXERCISE -ge 1 && $EXERCISE -le 19 ]]; then
      PENULTIMATE=$((EXERCISE-1))
      validResponse=1
    else 
      echo ""
      echo "Invalid response. Please re-enter a valid exercise number." 
      echo ""
    fi
  done  
} 

doCatchup(){
  if [[ $EXERCISE -gt 1 ]]; then
    ADVANCE_TO=exercise$PENULTIMATE
  else
    ADVANCE_TO=cleanup
  fi
  $DEVSH/scripts/advance_exercises.sh $ADVANCE_TO
  echo ""
  echo "You can now perform the" ${EXERCISES[$PENULTIMATE]} "exercise."
  echo ""
}

setExerciseNames
getStartState
doCatchup