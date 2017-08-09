-----------------------------------------------------------------------------------------------------------------------------------
$PMSourceFileDir/scripts/file_watcher.sh -f $PMTargetFileDir/triggers/polar_data_2_trigger.dat -n 60 -g N -e $$EMAIL_FAILURE_LIST
-----------------------------------------------------------------------------------------------------------------------------------


#!/bin/sh
function usage () {

cat << ! >&2

Usage:  $0 [
   	     -f  </home/user/TEST/GG/test.csv> | 
             -n  <no. of mins to wait> | 
             -g  <FLAG> |
             -e  <Email List> 
             ]

Info:

  options:
  -f 				  FILENAME  

  -n 			          COUNT

  -g 			          FLAG   

  -e                              EMAIL

  Sample command:
      ./file_watcher.sh -f /dwh/prd/app/tcs/starboard/starboard.csv  -n 10  -g Y -e user@email.com

!
exit -1
}

MY_SCRIPT_ARGS=$*
if [ -z "$MY_SCRIPT_ARGS" ]; then
    usage
fi

while getopts f:n:g:e: VALUE
do
        case $VALUE in
        f)
			export FILENAME="$OPTARG"
            ;;
        n)
			export COUNT="$OPTARG"
            ;;
        g)
                        export FLAG="$OPTARG"
            ;;
        e)
                        export EMAIL="$OPTARG"
            ;;
        *)
            echo "$ME: error: unrecognized FLAG [-$OPTARG]"
            usage
            exit -1
            ;;
        esac
done

if [[ -z $FILENAME ]] || [[ -z $COUNT ]] || [[ -z $FLAG ]] || [[ -z $EMAIL ]]  
then
     usage
     exit 1
fi

count1=`expr $COUNT \\* 6`

#echo "Using the following values for FTP arguments:"
#echo "filename: $FILENAME"
#echo "count: $COUNT"
#echo "flag: $FLAG"


a=0

while true;
do
a=$(($a+1))
 if  [[ -f  "$FILENAME" ]]; then
 exit 0 
 break
fi
if [[ ! -f $FILENAME && $a -eq $count1 && $FLAG = 'Y' ]]; then
 echo " File not found $FILENAME...Notify DWH  on call team" | mailx -s "File not found $FILENAME" $EMAIL 
fi
sleep 10
done


exit


