for file in $(cat $1);
do hdfs debug recoverLease -path "$file" -retries 3;
done > $1.log;
