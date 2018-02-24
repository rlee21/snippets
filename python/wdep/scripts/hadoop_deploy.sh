RUN_PATH=`dirname $0`
APP_PATH=$1

function run_cmd {
    cmd=$1

    echo "Running: $cmd"
    eval $cmd
}

cd $RUN_PATH
echo "Deploying data to hadoop: User = $USER, \
Gateway Path = $RUN_PATH, Hadoop Path = $APP_PATH"

if [[ $USER == 'jobrunner' ]]
then
    kinit $USER@CORP.AVVO.COM -k -t ~/$USER.keytab
fi

run_cmd "hdfs dfs -rm -f -R $APP_PATH"
run_cmd "hdfs dfs -mkdir $APP_PATH"
run_cmd "hdfs dfs -put ./* $APP_PATH/"
run_cmd "hdfs dfs -ls -R $APP_PATH"

