#!/bin/bash

retries=6
retry_sleep=$((60*10))
for i in `seq ${retries}`
do
    # Calls get_adw_accounts python script to download the Account Performance report from AdWords API.

    # Retry logic: error after ${retries} attempts, exit loop if successful
    if [[ $? == 0 ]]
    then
        break
    elif [[ ${i} == "$retries" ]]
    then
        echo "Error: AdWords get_adw_accounts.py failed (Retry: ${i})" >&2
        exit 1
    else
        echo "Error: AdWords get_adw_accounts.py failed, retrying get_adw_accounts.py (Retry: ${i})" >&2
        sleep $retry_sleep
    fi
done



