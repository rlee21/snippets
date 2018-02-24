#!/bin/bash

echo "The workflow has completed and now creating trigger file for successor jobs to run"

filepath="$1"
shift 1;
hadoop fs -mkdir -p $filepath

for filename in "$@"; do
    hadoop fs -touchz "$filepath/$filename"

    if [[ $? -ne 0 ]]
    then
     echo "The completion file [$filepath/$filename] hasn't been created"
    exit 1
    fi
done