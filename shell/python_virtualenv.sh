#!/bin/bash

# This script provides the functionality to execute any python script that
# would require additional python packages that are not installed in our
# Hadoop cluster.
# When deployed in gateway machine, this script will create virtualenv,
# download all packages needed (as specified in requirements.txt) locally
# to the gateway, and then package them all up and upload them to HDFS.
# When the Oozie action started this script for execution, this script will
# first download the package containing all python binaries and packages into
# local node and execute the given script using that python binary
# (essentially simulating virtualenv)

function printUsageAndExit {
  echo "Python deployment tool which allows usage of arbitrary packages"
  echo "Usage:"
  echo "To deploy all the required package/s (as specified on requirements.txt) to HDFS:"
  echo "  $0 -deploy <Workflow path in HDFS>"
  echo "To execute python script that needs such package/s from data node:"
  echo "  $0 <Workflow path in HDFS> <script.py>"
  exit 1
}

PYTHON_REQ_FILE="requirements.txt"
PYTHON_VIRTUAL_DIR="vPythonEnv"
PYTHON_TAR_FILE="vPythonEnv.tgz"

if [ $# -ge 1 ]; then
    if [ $1 == "-deploy" ]; then
      # Support deploy that will download packages and tarball it and deploy it to hadoop
      if [ $# -lt 2 ]; then
        printUsageAndExit
      fi
      HDFS_APP_PATH=$2
      TMP_DIR="$(mktemp -d)"

      # Make sure we clean up tmp folder when script completed
      trap 'echo Cleaning up "$TMP_DIR"...; rm -r -f "$TMP_DIR"; echo Done; exit' EXIT

      echo "Setting up virtual environment"
      echo "$TMP_DIR/$PYTHON_VIRTUAL_DIR"
      mkdir $TMP_DIR/$PYTHON_VIRTUAL_DIR
      virtualenv --python=python3.5 $TMP_DIR/$PYTHON_VIRTUAL_DIR

      echo "Installing all the required packages from requirements.txt"
      if [ ! -f $PYTHON_REQ_FILE ]; then
        echo "$PYTHON_REQ_FILE not found!"
        exit 1
      fi
      $TMP_DIR/$PYTHON_VIRTUAL_DIR/bin/pip install -r $PYTHON_REQ_FILE

      echo "Packaging all of the virtualenv binaries"
      PYTHON_TAR_FILE=$TMP_DIR/$PYTHON_TAR_FILE
      tar -zcf $PYTHON_TAR_FILE -C $TMP_DIR/ $PYTHON_VIRTUAL_DIR

      echo "Deploying tar file to hadoop"
      hdfs dfs -mkdir $HDFS_APP_PATH
      hdfs dfs -put $PYTHON_TAR_FILE $HDFS_APP_PATH/
    elif [ $1 == '-unpack' ] || [ $1 == '-run' ]; then
      export PYTHONPATH=`pwd`

      # Support execution that will download the tarball, unpack, and execute the python script
      HDFS_APP_PATH=$2

      echo "Download and unpack python virtual environment"
      rm $PYTHON_TAR_FILE
      hadoop fs -get $HDFS_APP_PATH/$PYTHON_TAR_FILE
      rm -rf $PYTHON_VIRTUAL_DIR
      tar -zxvf $PYTHON_TAR_FILE

    else
      echo "unknown arguments"
      exit 1
    fi

    if [ $1 == '-run' ]; then
      PYTHON_SCRIPT_FILE=$3

      shift 3

      echo "Executing python script ($PYTHON_SCRIPT_FILE) from virtual environment"
      $PYTHON_VIRTUAL_DIR/bin/python $PYTHON_SCRIPT_FILE "$@"
    fi
else
    printUsageAndExit
fi
