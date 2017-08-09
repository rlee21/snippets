#!/bin/ksh
#
##################################################################################
# edw_extract_start.ksh
# This script executes bteq script/Macro for EDW.
# This script check the prior load status(completed/incompleted) for EDW.
# This script insert the record into the table before the FTP start. 
# This script also register the FTP completion status in the CFW tables.
##################################################################################
#set -x


load_failure()
{

if [[ -f ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_failure} ]]
    then
      echo "Executing command from the file ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_failure}\n"
      ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_failure}
	  RC=$?
      if [ $RC -gt 0 ]
      then
         echo "[ERROR]: ${SQLName_post_load_failure} failed \n" 
         echo "${SQLName_post_load_failure} failed\n"
      else
         echo "${SQLName_post_load_failure} completed successfully\n"
		 echo "and ${SQLName_post_load_failure} BTEQ Script is used to update xtr_run_sta_cde_id with failed status 6, in SRC_SYS_XTR_RUN table \n"
		 echo "Check $0.${src_sys_xtr_id}.${LOGDATESTAMP} LOG FILE for error\n"
    fi
  else
      echo "BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_failure} does not exist\n"
  fi

}

###################################################################################
#------------------------------------Main-----------------------------------------#
###################################################################################

if [ $# -lt 2 ]
then
        echo usage:
        echo $0 [src_sys_xtr_id] NotifySrcName [ParFile] [var1] [var2] [var3]
        echo e.g.: $0 edwhmdm [src_sys_xtr_id] and [NotifySrcName]
exit 1
fi

# Processing of arguments...
export src_sys_xtr_id="$1"
export NotifySrcName="$2"
export VAR3="$3"
export VAR4="$4"
export VAR5="$5"
export SQLName="GENERATE_TASK_PARM.bteq"
export SQLName_load_check="CALL_PRIOR_LOAD_COMPLETION_CHECK.bteq"
export SQLName_post_load_completion="CALL_POST_LOAD_COMPLETION.bteq"
export SQLName_post_load_failure="CALL_POST_LOAD_FAILURE.bteq"

#echo $0     # Print the script name

#PROGDI=$(dirname $0)
PROGDIR=/informatica/powercenter/Dev/Workflows/EDW/Scripts
export LOCAL_BASE=${PROGDIR%/*}


#######################################################################
# Loading environment variables
#######################################################################
. $PROGDIR/.etlvars.env

export PROGNAME=`basename $0`
export LOGDATESTAMP=`date '+%Y%m%d%H%M%S'`

########################################################################
# Log file Creation
########################################################################

 export LOGFILE="${PMSessionLogDir}/$0.${src_sys_xtr_id}.${LOGDATESTAMP}"
 echo "Log File Name $0.${src_sys_xtr_id}.${LOGDATESTAMP} \n" >> ${LOGFILE} 
########################################################################
# Email addresses
########################################################################
 
 export ADDRESSEES="$(cat ${LOCAL_BASE}/${TXT_PATH}/${NotifySrcName}_notify_email.lst)"

########################################################################
# Redirect stdout and stderr to the logfile 
######################################################################## 
 
 echo Redirecting ${PROGNAME} output to $LOGFILE
 exec >> "${LOGFILE}" 2>&1
 date

#######################################################################
# Check if BTEQ script exists.If exists then execute it.
#######################################################################
  
  if [[ -f ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName} ]]
    then
      echo "Executing command from the file ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName}"
      ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName}
	  RC=$?
      if [ $RC -gt 0 ]
      then
         echo "[ERROR]: ${SQLName} failed"
         echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-1]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
         MSG="${SQLName} failed"
		 mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} from  `hostname`: $PROGNAME for ${SQLName} BTEQ script" $ADDRESSEES < $LOGFILE
		 exit $RC
      else
         echo "${SQLName} completed successfully\n"
         MSG="${SQLName} completed successfully"
    fi
  else
      echo "BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName} does not exist"
      echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-1]in FTP_RECOVERY_STEPS Document\n"
      echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
      MSG="BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName} does not exist"
      mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME for ${SQLName} BTEQ script" $ADDRESSEES < $LOGFILE
	  RC=2
	  exit $RC
  fi 

######################################################################## 
# Loading parameters if param file exists
########################################################################
 
if [[ -f ${PMParFileDir}/GENERATE_${src_sys_xtr_id}_PARM.txt ]]
 then
    echo "Loading Parameters from ${PMParFileDir}/GENERATE_${src_sys_xtr_id}_PARM.txt file\n"
	 . /$PMParFileDir/GENERATE_${src_sys_xtr_id}_PARM.txt
 else
    echo "[ERROR]: Param file $PMParFileDir/GENERATE_${src_sys_xtr_id}_PARM.txt does not exist\n"
    echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-2]in FTP_RECOVERY_STEPS Document\n"
    echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
    mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME.GENERATE_${src_sys_xtr_id}_PARM.txt param file not found" $ADDRESSEES < $LOGFILE
	exit 1	
 fi

###########################################################################
#Checking if all the parameters values and directories exists.
#Checking if Max Wait Time and Sleep Time are valid numbers
###########################################################################

echo "+--------+--------+--------+--------+-------+-------+-------+"
echo "...............VARIABLES/PARAMETERS VALIDATION..............."
echo "+--------+--------+--------+--------+-------+-------+-------+"

if [ [${src_fil_lod_loc_txt} != ""] -a [${edw_fil_loc_txt} != ""] -a [${edw_lyr_abr} != ""] -a [${edw_lyr_id} != ""] -a [${fed_typ_cde} != ""] -a [${fed_typ_des} != ""] -a [${max_wt_tme_min_ct} != ""] -a [${sleep_tme_min_ct} != ""] -a [${edw_src_fil_loc_txt} != ""] -a [${src_sys_cde} != ""] -a [${src_sys_id} != ""] -a [${src_sys_xtr_id} != ""] -a [${tsk_id} != ""] -a [${src_sys_xtr_sta_cde_id} != ""] -a [${src_xtr_nme} != ""] -a [${sta_cde_abr} != ""] -a [${tsk_nme} != ""] -a [${tsk_typ_id} != ""] -a ['${tsk_typ_nme}' != ""] ]
 then
  if [ -d ${edw_fil_loc_txt}  and -d ${edw_src_fil_loc_txt} and -d ${src_fil_lod_loc_txt} ]
   then
    echo "All the variables values and File location exist\n"
  else
    echo "[ERROR]: File locations does not exist  \n"
    echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-3]in FTP_RECOVERY_STEPS Document\n"
    echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
	mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME.File location does not exist" $ADDRESSEES < $LOGFILE
    exit 1 
  fi
else
 echo "[ERROR]: All the variables does not have value/File location does not exist \n"
 echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-3]in FTP_RECOVERY_STEPS Document\n"
 echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
 mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME.Some of the parameters does not have value" $ADDRESSEES < $LOGFILE
 exit 1
fi

#max_wt_tme_min_ct="j"

if [[ ${max_wt_tme_min_ct} != [0-9]* || ${sleep_tme_min_ct} != [0-9]* ]]
then 
   echo "Either Sleep Time or Maximum Wait Time are non-numeric\n" 
   echo "Please resubmit with right arguments.Check AUDIT_CTL_V_1_0.SRC_SYS_XTR table \n"
   echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-4]in FTP_RECOVERY_STEPS Document\n"
   echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
   mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME for Sleep Time/Maximum Wait Time(Invalid Numbers)" $ADDRESSEES < $LOGFILE
   exit 1
else
   if [ ${sleep_tme_min_ct} -gt ${max_wt_tme_min_ct} ]
   then
      echo "[ERROR]: Sleep Time must be less than or equal to Maximum Wait Time\n"
      echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-4]in FTP_RECOVERY_STEPS Document\n"
      echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
      mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME for Sleep Time/Maximum Wait Time(Sleep time <= Max Wait Time)" $ADDRESSEES < $LOGFILE
      exit 1
   else
     echo "You have specified ${max_wt_tme_min_ct} seconds as Maximum Wait Time and ${sleep_tme_min_ct} seconds as the Sleep Time\n"
   fi
fi
  

##############################################################################
#Check for the Prior loads completion
##############################################################################

export sys_xtr_run_lod_dtm=`date '+%Y-%m-%d %H:%M:%S'`

echo "$sys_xtr_run_lod_dtm"

if [[ -f ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_load_check} ]]
    then
	echo "Executing command from the file ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_load_check}"
    ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_load_check}
    RC=$?
      if [ $RC -gt 0 ]
      then
         echo "[ERROR]: ${SQLName_load_check} failed"
         echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-5]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
         MSG="${SQLName_load_check} failed"
		 mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME for ${SQLName_load_check} BTEQ script" $ADDRESSEES < $LOGFILE
		 exit $RC
      else
         echo "${SQLName_load_check} completed successfully\n"
         MSG="${SQLName_load_check} completed successfully"
	 fi
else
      echo "[ERROR]: BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_load_check} does not exist"
      echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-5]in FTP_RECOVERY_STEPS Document\n"
      echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
      MSG="BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_load_check} does not exist"
      mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id}, from  `hostname`: $PROGNAME for ${SQLName_load_check} BTEQ script" $ADDRESSEES < $LOGFILE
	  RC=2
	  exit $RC
fi 

####################################################################################
# FTP the files from a different Server -- PULL
####################################################################################

cd ${edw_src_fil_loc_txt}

if [ ${fed_typ_cde} = "P" ]
  then

CtlFilCnt_OnlclDir=`ls ${ctl_fil_nme}.* | wc -l`
i=0
while [[ $CtlFilCnt_OnlclDir -eq 0 && $i -le $max_wt_tme_min_ct ]] 
  	do  
   echo "`date '+%Y%m%d%H%M%S'`"
   export SERVER_NAME=`eval echo '$'${src_sys_cde}_SERVER`
   export USER_NAME=`eval echo '$'${src_sys_cde}_USER`
   export PSWD=`eval echo '$'${src_sys_cde}_PWD`
   #echo "\n ${PI_SERVER} ${PI_USER}\n"
   ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/edw_exec_ftp.ksh ${SERVER_NAME} ${USER_NAME} ${PSWD} ${src_fil_ftp_loc_txt} mget ${ctl_fil_nme}.*
   RC=$?
   
   if [ $RC -gt 0 ]
     then
     load_failure
     echo "[ERROR]: edw_exec_ftp.ksh Failed while FTP the ${ctl_fil_nme}.* control file"
     echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-6]in FTP_RECOVERY_STEPS Document\n"
     echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
     MSG="edw_exec_ftp.ksh Failed while FTP the ${ctl_fil_nme}.* control file "
     mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm}, from  `hostname`: $PROGNAME.FTP Failed because edw_exec_ftp.ksh script Failed" $ADDRESSEES < $LOGFILE
     exit $RC
   else
     echo "Successfully executed edw_exec_ftp.ksh script for ${ctl_fil_nme}.* control file\n" 
   fi
   
   CtlFilCnt_OnlclDir=`ls ${ctl_fil_nme}.* | wc -l`
    
   if [[ $CtlFilCnt_OnlclDir -ge 1 ]]
     then
      ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/edw_exec_ftp.ksh ${SERVER_NAME} ${USER_NAME} ${PSWD} ${src_fil_ftp_loc_txt} mget ${src_xtr_nme}.*
      RC=$?
      
       if [ $RC -gt 0 ]
         then
         load_failure
         echo "[ERROR]: edw_exec_ftp.ksh Failed while FTP the ${src_xtr_nme}.* data file "
         echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-6]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
         MSG="edw_exec_ftp.ksh Failed while FTP the ${src_xtr_nme}.* data file "
         mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from  `hostname`: $PROGNAME.FTP Failed because edw_exec_ftp.ksh script failed" $ADDRESSEES < $LOGFILE
         exit $RC
       else
        echo "Successfully executed edw_exec_ftp.ksh script for ${src_xtr_nme}.* data file " 
       fi
      
   else 
     sleep $sleep_tme_min_ct 
     i=`expr $i + $sleep_tme_min_ct`
   fi

done	

if [[ $CtlFilCnt_OnlclDir -eq 0 && $i -ge $max_wt_tme_min_ct ]]
 then
   load_failure
   echo "[ERROR]:Control file ${ctl_fil_nme}.* not available in FTP folder "
   echo "[ERROR]:The specified time limit of wait time was specified as $max_wt_tme_min_ct seconds. Control file ${ctl_fil_nme}.* not found during this time."
   echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-7]in FTP_RECOVERY_STEPS Document\n"
   echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
   MSG="Control file ${ctl_fil_nme}.* not available in FTP folder"
   mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from  `hostname`: $PROGNAME.FTP Failed because ${ctl_fil_nme}.* file not available in FTP folder and Max wait time $max_wt_tme_min_ct second is over" $ADDRESSEES < $LOGFILE
   RC=1
   exit $RC
fi   

fi


###################################################################################
# Looking for File on our local directory -- LOOK
###################################################################################


if [ ${fed_typ_cde} = "L" ]
  then

echo "\n LOOKING FOR CONTROL FILE IN THE LOCAL DIRECTORY \n"

CtlFilCnt_OnlclDir=`ls ${ctl_fil_nme}.* | wc -l`
i=0
while [[ ${CtlFilCnt_OnlclDir} -ne 1 && $i -le $max_wt_tme_min_ct ]] 
   do
      sleep $sleep_tme_min_ct
	  echo "`date '+%Y%m%d%H%M%S'`"
      CtlFilCnt_OnlclDir=`ls ${ctl_fil_nme}.* | wc -l`
      i=`expr $i + $sleep_tme_min_ct`
   done
   
if [ ${CtlFilCnt_OnlclDir} -eq 0 ]
 then
   load_failure
   echo "[ERROR]: The specified time limit of wait time was specified as $max_wt_tme_min_ct seconds. Control file ${ctl_fil_nme}.* not found during this time\n"
   echo "Process will now be terminated\n"
   echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-8]in FTP_RECOVERY_STEPS Document\n"
   echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
   MSG="Control file ${ctl_fil_nme}.* not available in local folder"
   mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from `hostname`: $PROGNAME.Control file ${ctl_fil_nme}.* file not available in local folder and Max wait time $max_wt_tme_min_ct is over" $ADDRESSEES < $LOGFILE
   RC=1
   exit $RC
fi
       
fi

######################################################################################################
# Reading date part from the control file.Calculating number of records in the source file.
# Copying the source file to loading location.Zipping the source file and moving it to Archive folder.
# Removing the control file and data file from FTP as well as from EDW landing area.
###################################################################################################### 

if [ ${CtlFilCnt_OnlclDir} -eq 0 ]
 then
   load_failure
   echo "[ERROR]: The specified time limit of wait time was specified as $max_wt_tme_min_ct seconds.Control file ${ctl_fil_nme}.* not found during this time.\n"
   echo "\nProcess will now be terminated"
   echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-9]in FTP_RECOVERY_STEPS Document\n"
   echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
   MSG="Control file ${ctl_fil_nme}.* not available in local folder"
   mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from  `hostname`: $PROGNAME.Control file ${ctl_fil_nme}.* not available in local folder" $ADDRESSEES < $LOGFILE
   RC=1
   exit $RC
else
   export xtr_run_dta_fil_nme=`ls ${src_xtr_nme}.*`  
   export xtr_run_ctl_fil_nme=`ls ${ctl_fil_nme}.*`
   export xtr_run_ctl_fil_dtm=`echo ${xtr_run_ctl_fil_nme} | cut -f3 -d'.'`
   YYYY=`echo ${xtr_run_ctl_fil_dtm}|cut -c1-4`
   MM=`echo ${xtr_run_ctl_fil_dtm}|cut -c5-6`
   DD=`echo ${xtr_run_ctl_fil_dtm}|cut -c7-08`
   HH=`echo ${xtr_run_ctl_fil_dtm}|cut -c9-10`
   MN=`echo ${xtr_run_ctl_fil_dtm}|cut -c11-12`
   SS=`echo ${xtr_run_ctl_fil_dtm}|cut -c13-14`
   export xtr_run_dta_fil_row_ct=`wc -l ${xtr_run_dta_fil_nme} | sed 's/ //g' | sed 's/'${xtr_run_dta_fil_nme}'//g'`
   export src_xtr_dtm=`echo ${YYYY}-${MM}-${DD} ${HH}:${MN}:${SS}`
   echo " \nsrc_xtr_dtm is ${src_xtr_dtm}"
   echo "Total number of records in ${xtr_run_dta_fil_nme} file=${xtr_run_dta_fil_row_ct}\n"
   echo "${xtr_run_ctl_fil_nme} control file received in ${edw_src_fil_loc_txt} directory\n"   
   
   echo "Copying ${xtr_run_dta_fil_nme} file from landing to loading Directory\n"  
   
   cp ${edw_src_fil_loc_txt}/${xtr_run_dta_fil_nme} ${src_fil_lod_loc_txt}/${xtr_run_dta_fil_nme}
   RC=$?
   
   if [ $RC -gt 0 ]
     then
            load_failure
            echo "[ERROR]:Copy command failed while coping the ${xtr_run_dta_fil_nme} file from Landing Directory to Loading Directory\n"
            echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-10]in FTP_RECOVERY_STEPS Document\n"
            echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"
            mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from `hostname` :$PROGNAME .Copy command failed while coping the ${xtr_run_dta_fil_nme} file" $ADDRESSEES < $LOGFILE
            exit $RC  
   else
            echo "Successfully copied ${xtr_run_dta_fil_nme} data file to the Loading directory\n"
   fi  
   
   
   echo "gzip ${xtr_run_dta_fil_nme} file \n"
   gzip ${xtr_run_dta_fil_nme}
   RC=$?
   
   
   if [ $RC -eq 0 ]
     then
       mv ${xtr_run_dta_fil_nme}.gz ${edw_fil_loc_txt}
       RC=$?
         if [ $RC -eq 0 ]
           then
            echo "Data File ${xtr_run_dta_fil_nme} is moved to the Loading folder and zipped in Archive folder\n" 
         else
            load_failure
            echo "[ERROR]: Move command failed while moving the ${xtr_run_dta_fil_nme} file from Landing Directory to Loading Directory\n"
            echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-10]in FTP_RECOVERY_STEPS Document\n"
            echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
            mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from `hostname` :$PROGNAME . Move command failed while moving the ${xtr_run_dta_fil_nme} file" $ADDRESSEES < $LOGFILE
            exit $RC 
         fi       
   else
      load_failure
      echo "[ERROR]: ZIP command failed while zipping the ${xtr_run_dta_fil_nme} file in Archive directory\n"
      echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-10]in FTP_RECOVERY_STEPS Document\n"
      echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
      mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from `hostname` :$PROGNAME .ZIP command failed while zipping the ${xtr_run_dta_fil_nme} file" $ADDRESSEES < $LOGFILE
      exit $RC 
   fi
 
   
if [ ${fed_typ_cde} = "P" ]
  then
  
  ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/edw_exec_ftp.ksh ${SERVER_NAME} ${USER_NAME} ${PSWD} ${src_fil_ftp_loc_txt} mdelete ${xtr_run_ctl_fil_nme}
  RC=$?
  
  if [ $RC -gt 0 ]
  then
     load_failure
     echo "[ERROR]: edw_exec_ftp.ksh script Failed while deleting the ${xtr_run_ctl_fil_nme} control file from ${PI_SERVER} server\n"
     echo "FTP Server Name : ${PI_SERVER}   and   FTP Location : ${src_fil_ftp_loc_txt} \n"
     echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-11]in FTP_RECOVERY_STEPS Document\n"
     echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
	 mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} , from `hostname` :$PROGNAME .edw_exec_ftp.ksh script failed while deleting the files." $ADDRESSEES < $LOGFILE
     exit $RC
  else
     ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/edw_exec_ftp.ksh ${SERVER_NAME} ${USER_NAME} ${PSWD} ${src_fil_ftp_loc_txt} mdelete ${xtr_run_dta_fil_nme}
	 RC=$?
	  if [ $RC -gt 0 ]
	  then
	    load_failure
	    echo "[ERROR]: edw_exec_ftp.ksh script Failed while deleting the ${xtr_run_dta_fil_nme} control file from ${PI_SERVER} server\n"
        echo "FTP Server Name : ${PI_SERVER}   and   FTP Location : ${src_fil_ftp_loc_txt} \n"
        echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-11]in FTP_RECOVERY_STEPS Document\n"
        echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
	    mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from `hostname` :$PROGNAME .edw_exec_ftp.ksh script failed while deleting the files." $ADDRESSEES < $LOGFILE
        exit $RC
	  else
	   echo "Successfully deleted control and data files from FTP server.\n"
	  fi
  fi
  
  rm -f ${edw_src_fil_loc_txt}/${xtr_run_ctl_fil_nme}  # Remove files from Loading Directory if fed_typ_cde= PULL
  RC1=$?
  rm -f ${edw_src_fil_loc_txt}/${xtr_run_dta_fil_nme}
  RC2=$?
    
  let RC=RC1+RC2
  
      if [ $RC -gt 0 ]
       then
         load_failure
         echo "[ERROR]: Remove command failed while removing the ${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files from Loading Directory\n"
         echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-11]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
         mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from `hostname` :$PROGNAME .Remove command failed while removing ${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files" $ADDRESSEES < $LOGFILE
         exit $RC   
      else
         echo "${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files are removed successfully from loading Directory\n" 
      fi
  
  
else
  rm -f ${edw_src_fil_loc_txt}/${xtr_run_ctl_fil_nme}        # Remove files from Loading Directory if fed_typ_cde= LOOK
  RC1=$?
  rm -f ${edw_src_fil_loc_txt}/${xtr_run_dta_fil_nme}
  RC2=$?
      
  let RC=RC1+RC2
  
      if [ $RC -gt 0 ]
       then
         load_failure
         echo "[ERROR]:Remove command failed while removing the ${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files from Loading Directory\n"
         echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-12]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
         mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from `hostname` :$PROGNAME .Remove command failed while removing ${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files" $ADDRESSEES < $LOGFILE
         exit $RC   
      else
         echo "${xtr_run_ctl_fil_nme} and ${xtr_run_dta_fil_nme} files are removed successfully\n" 
      fi  
  
fi  
  
  
fi 


#################################################################################
# Post FTP completion.Update the actual file names.
# Update total number of record counts in AUDIT_CTL_V_1_0.SRC_SYS_XTR_RUN table.
#################################################################################

if [[ -f ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_completion} ]]
    then
	echo "Executing command from the file ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_completion}"
    ksh ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_completion}
    RC=$?
      if [ $RC -gt 0 ]
      then
         load_failure
		 echo "[ERROR]: ${SQLName_post_load_completion} failed\n"
		 echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-13]in FTP_RECOVERY_STEPS Document\n"
         echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************"  
         MSG="${SQLName_post_load_completion} failed"
		 mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from  `hostname`: $PROGNAME for ${SQLName_post_load_completion} BTEQ script" $ADDRESSEES < $LOGFILE
		 exit $RC
      else
         echo "${SQLName_post_load_completion} completed successfully\n"
         MSG="${SQLName_post_load_completion} completed successfully"
	 fi
else
      load_failure
	  echo "BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_completion} does not exist\n"
	  echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-13]in FTP_RECOVERY_STEPS Document\n"
      echo "RECOVERY DOCUMENT LINK: 'https://teams.ghc.org/eim/sites/dw/_layouts/WordViewer.aspx?id=/eim/sites/dw/Shared%20Documents/Technical%20Operations/Recovery%20Documentation/FTP_RECOVERY_STEPS.doc&Source=https%3A%2F%2Fteams%2Eghc%2Eorg%2Feim%2Fsites%2Fdw%2FShared%2520Documents%2FForms%2FAllItems%2Easpx%3FRootFolder%3D%252Feim%252Fsites%252Fdw%252FShared%2520Documents%252FTechnical%2520Operations%252FRecovery%2520Documentation%26InitialTabId%3DRibbon%252EDocument%26VisibilityContext%3DWSSTabPersistence&DefaultItemOpen=1'\n***************************" 
      MSG="BTEQ script ${LOCAL_BASE}/${SCRIPTS_PATH}/SQL/${SQLName_post_load_completion} does not exist"
      mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm} ,from  `hostname`: $PROGNAME for ${SQLName_post_load_completion} BTEQ script" $ADDRESSEES < $LOGFILE
	  RC=2
	  exit $RC
fi 


echo "-------------------SUCCESS------------------------SUCCESS-------------------SUCCESS---------------SUCCESS---------------SUCCESS---------------SUCCESS---------" 

echo "Required control file: ${xtr_run_ctl_fil_nme} and data file: ${xtr_run_dta_fil_nme} have been received in process directory. Process will continue to next step" 

echo "-------------------SUCCESS------------------------SUCCESS-------------------SUCCESS---------------SUCCESS---------------SUCCESS---------------SUCCESS---------" 
echo "\n Process end time is `date`"


echo "\n ***Successful FTP(LOOK/PULL) completion Message from `hostname`: $PROGNAME for Source System Extract Id : ${src_sys_xtr_id} ***\n"

mailx -s " *** FTP(LOOK/PULL) Success Message from `hostname`: $PROGNAME for Source System Extract Id : ${src_sys_xtr_id} and System Extract Run Load DateTime: ${sys_xtr_run_lod_dtm} ***" $ADDRESSEES < $LOGFILE


################################### End of Main ##########################################

 