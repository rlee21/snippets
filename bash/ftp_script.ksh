#!/bin/ksh
#
######################################################################
# Name          :  edw_exec_ftp.ksh
# Description   :  This script used for FTP the files 
# Called by     :  edw_extract_start.ksh
# Author        :  
# Date          :  
######################################################################
#set -x 

export PI_SERVER=$1
export PI_USER=$2
export PI_PWD=$3
export FTP_PATH=$4
export COMMAND=$5
export FILE_NME=$6


PROGDIR=$(dirname $0)
export LOCAL_BASE=${PROGDIR%/*}
#export LOCAL_BASE=/informatica/powercenter/Dev/Workflows/EDW/

   ftp -nv $PI_SERVER <<EOF
      user $PI_USER $PI_PWD
      prompt off
      cd ${FTP_PATH}
      ${COMMAND} ${FILE_NME}
   bye
 EOF
date
exit
