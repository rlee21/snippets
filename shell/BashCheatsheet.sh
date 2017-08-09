set -x - debug
ps -ef | grep UID | more -see processes running for a user
pkill -u UID - kill all processes for a user
sudo - allows a user to execute a command as another user
ls - list files
chmod - change directory/file attributes
find - searches for files on disk or for strings inside of files
cp - copy files; used to backup files or create multiple versions (FTP)
mv - move/rename file (FTP)
rm - remove file (FTP)
cd - change directory (FTP)
pwd - print working directory
more - displays a page at a time (pauses as each page is displayed) (FTP w/textpad)
cat - combines multiple files in the same format into 1 file
state - determine if a file is on disk
vi - used to edit WF / DM system files (FTP w/textpad) *** :wq! or :q ***
jar - create or extract java archive files
cron/crontab - create/maintain schedules in UNIX
topas - system monitoring
mkdir/rmdir - create/remove directories (FTP)
df - display used/available space disk file system
java - display java version, execute java commands (dserver.xmls)
passwd - change user’s password
unzip - extract files from a compressed file
date - display UNIX date and time
echo - display the value of a variable
uname -a - version of Unix
env - displays the current environment
printenv - displays environmental variables
vi /home/informatica/.bash_profile - set environmental variable
export INFA_SHARED=/opt/informatica/9.5.1/server/infa_shared - set environmental variable
unset INFA_SHARED - removes environmental variable
set - displays environment variable
ex - execute a program (used in UNIX scripts)
grep - used to search for a string value inside files
man - display the manual for specific UNIX commands
mail - read/delete mail received by ‘eda’ (output of processes)
mail -s "TestSubject" nospam@gmail.com -a "UserReport.txt"  < MessageBody.txt
 # -s flag is used for "Subject" and -a flag is used for "Attachment file"
 # The above command is not finished upon hitting Enter. Next you have to type in the message. When you're done, hit 'Ctrl-D' at the beginning of a line
ping - verify access to a server
netstat - show network status
nohup - used to execute ReportCaster server
ps - list UNIX processes; needed to determine of processes running
tail - read the end of a file
du -sc * | sort -n | tail - lists the sizes of all files and directories sorted from smallest to largest in size
df -k space utilization
wc -l line count
cat MDWHBOOKU.dat | sed 's/\,/ /'|awk '{print $1}' > test.dat - selecting a column in flat file
ulimit -u - maximum number of processes that can run
ulimit -a show all 
scp * informatica@haldevinf01:<dir>
sudo chown informatica:informatica *
awk - cat /opt/informatica/9.5.1/server/infa_shared/SrcFiles/filelists/ff_MASTER_FILELIST.dat | grep "s_m_Load_STG_DWHPPIF ETL_GROUP_PCL" | awk -F ' ' '{print $1}'
awk '{print $1","$2}' flat_file_counts_tmp.dat > flat_file_counts.dat; - create delimited file (example is comma separated)
whoami
chmod -R 0777 triggers/
$ nohup ./my-shell-script.sh & - Execute a command in the background using nohup
:1,$d - to delete all text in vi
:q! - to quit if you haven't made any edits.
:wq - to quit and save edits (basically the same as ZZ).
:%s/PRDEDW1/STGDWH1/g - search and replace all
zip - zip filename.zip input1.txt input2.txt resume.doc pic1.jpg 
gzip -c DOWN9114S.dat > DOWN9114S.ZIP.`date +%A| sed 's/.*/\U&/'`    - Zip with Day of week upper case name
gpg --decrypt a.txt.gpg > secret.txt
rmdir directory -  remove dir
chmod -R 777 dir - change folder permissions
cp -R SRCFOLDER DESTFOLDER/ - copy folder with files to another folder
cat file1 file2 file3 > newfile   - concatenate multiple files into a single file
ls -1 | wc -l count of files current directory
chmod 775
gunzip
tar -xvf
tar zxvf file_name.tar.gz - uncompress tar.gz file
tar -zcvf archive.tar.gz directory/  - "zip" a directory, the correct command would be
gunzip -c ROLMANIFEST.tar.gz.TODAY | tar xvf -
gzip -d HALADP20150823.csv.gz - uncompress csv.gz file
cat /etc/passwd - list of users
grep -U $'\015' DWPROVOY.dat - CRLF character
unzip -o /opt/informatica/dwh/app/tcs/polar_test_feeds/HAQDATA.zip -d /opt/informatica/dwh/app/tcs/pcl2hal/
ln -s {/path/to/file-name} {link-name} - create symbolic link
rm {link-name} - remove symbolic link
Command to convert the windows format to UNIX format
tr -d '\15\32' < DWHCOMPB.dat > UNIX_DWHCOMPB.dat
passing to script : export PARM_1=`eval echo ${1}`
iconv -f UTF-8 -t ascii//TRANSLIT PAXDATA_NO_20160525_131924.TXT
file --mime-encoding DWHSSI.dat or file -bi DWHSSI.dat - to see encoding
iconv -f iso-8859-1 -t utf-8 DWHSSI.dat > DWHSSI_NEW.dat - to change encoding
sftp  -oPort=1002 -oIdentityFile=/home/informatica/.ssh/sabre_sftp sabre@halprdftp01
To convert a Windows file to a Unix file, enter:
  dos2unix winfile.txt unixfile.txt
To convert a Unix file to Windows, enter:
  unix2dos unixfile.txt winfile.txt
To use awk to convert a Windows file to Unix, enter:
  awk '{ sub("\r$", ""); print }' winfile.txt > unixfile.txt
To convert a Unix file to Windows, enter:
  awk 'sub("$", "\r")' unixfile.txt > winfile.txt  
--determine if file is empty or not
[ -s test.txt ] && echo "File not empty" || echo "File empty"
#!/bin/bash -e

if [ -s file_name.txt ]
then
        rm -f empty.txt
        touch full.txt
else
        rm -f full.txt
        touch empty.txt
fi
------
RC=$?
if [[ $RC != 0 ]]; then
echo 'rename failure'
exit 1
fi
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Cron Job
minute	This controls what minute of the hour the command will run on,
	 and is between '0' and '59'
hour	This controls what hour the command will run on, and is specified in
         the 24 hour clock, values must be between 0 and 23 (0 is midnight)
dom	This is the Day of Month, that you want the command run on, e.g. to
	 run a command on the 19th of each month, the dom would be 19.
month	This is the month a specified command will run on, it may be specified
	 numerically (0-12), or as the name of the month (e.g. May)
dow	This is the Day of Week that you want a command to be run on, it can
	 also be numeric (0-7) or as the name of the day (e.g. sun).
user	This is the user who runs the command.
cmd	This is the command that you want run. This field may contain 
	 multiple words or spaces.
	 
If you don't wish to specify a value for a field, just place a * in the 
field.	 

The number of minutes after the hour (0 to 59)
The hour in military time (24 hour) format (0 to 23)
The day of the month (1 to 31)
The month (1 to 12)
The day of the week(0 or 7 is Sun, or use name)
The command to run	
------------------------------------------------
--shell script
#!/bin/sh
cd /opt/informatica/9.5.1/server/infa_shared/SrcFiles/pcl2hal
wc -l *.dat > flat_file_counts_tmp.dat
awk '{print $1","$2}' flat_file_counts_tmp.dat > flat_file_counts.dat
rm flat_file_counts_tmp.dat
cd /opt/informatica/9.5.1/server/infa_shared/SrcFiles/pcl2sbn
wc -l *.dat > flat_file_counts_tmp.dat
awk '{print $1","$2}' flat_file_counts_tmp.dat > flat_file_counts.dat
rm flat_file_counts_tmp.dat
cd /opt/informatica/9.5.1/server/infa_shared/SrcFiles/pcl2sit/robert
wc -l *.dat > flat_file_counts_tmp.dat
awk '{print $1","$2}' flat_file_counts_tmp.dat > flat_file_counts.dat
rm flat_file_counts_tmp.dat
------------------------------------------------
/* sudo/dzdo example */
After signon as yourself, use "sudo su - eda", use your same password when it prompts you.

Example:  dzdo su - eda


/* lists example */
ls -la  lists all files in current directory
ls -lat  list all files in current directory in sequence by most-recently modified.
ls -la *  list all files in current and sub-directories
ls -lrt
ls -la * > savefile.txt list all files in current and sub-directories and store results in savefile.txt

/* chmod's */ 
-rwxrwxr-x 775
-rwxr----- 740
-rw-rw-r-- 664
-rw------- 600

Example:  tstwf01:/dwh/tst/app/dm/app>chmod 775 savefile.txt 


/* VI Display contents of a text file */
http://www.lagmonster.org/docs/vi.html
https://www.ccsf.edu/Pub/Fac/vi.html
vi file_name.txt

Quitting and Saving a File

The command ZZ (notice that it is in uppercase) will allow you to quit vi and save the edits made to a file. You will then return to a Unix prompt. Note that you can also use the following commands:

:w	to save your file but not quit vi (this is good to do periodically in
	case of machine crash!).
:q	to quit if you haven't made any edits.
:wq	to quit and save edits (basically the same as ZZ).
Quitting without Saving Edits

Sometimes, when you create a mess (when you first start using vi this is easy to do!) you may wish to erase all edits made to the file and either start over or quit. To do this, you can choose from the following two commands:

:e!	reads the original file back in so that you can start over.
:q!	wipes out all edits and allows you to exit from vi.

/*Closing file
ESC, type :q and then press ENTER


/* grep example */
grep -irl <string> dir/ 
find . -type f | grep fex | xargs grep -irl <string>

--------------------------
/*Tail a log, live, and watch log entries get created:*/
tail -f /path/to/log/file.log

/*List running processes on the box:*/
ps -ef 

/*With that you can always pipe that through a grep command to see if something specific is running:*/
ps -ef | grep process_name

/*List all "normal" files in the current directory and sub directories:*/
find . -type f

/*With that you can grep for a specific file:*/
find . -type f | grep filename

/*Move down a directory:*/
cd ..

/*Kill a command (get back to a prompt):*/
CTRL+C

/*Start an informatica workflow as the "Informatica" user (latest checked-in version of a map/session/wf):*/
pmcmd startworkflow -sv $INFA_IS -d $INFA_DOMAIN -uv INFA_USER -pv INFA_PASSWORD -f a_Common_Objects -lpf /opt/informatica/9.5.1/server/infa_shared/SrcFiles/parms/parameters_start_PRDEDW1.txt -wait wkf_Load_DWH

/* Search for specific file types */
find <directory> -name "*.file_extension"

/*Sometimes, I’ll search for all files, then want to grep those files.  For example, if I wanted to grep the TCS directory for "Robert", but only wanted to search .fex files (and ignore .dat files, for example).. "xargs" will use the output of the previous command as input for the next:*/
find /dwh/prd/app -type f | grep fex | xargs grep -i ROBERT
--------------------------------------------------------------------------------
#!/bin/sh

export DIR_PATH="$INFA_SHARED/SrcFiles/sabre"

TF="hal_sabredone.dat"

cd $DIR_PATH
FDATE=`cat $TF`
unzip -o HAL_BATCH_EXTRACT_${FDATE}_DAT.ZIP
FDATE=`cat $TF`
for file in `ls *_${FDATE}.dat`
do
newfile=`echo $file | sed s/_$FDATE//`
mv $file $newfile
done



gpg --edit-key "All Brands Group"
command>trust

5

y

RC=$?
 if [ $RC -gt 0 ]
     then
     load_failure
     echo "[ERROR]: edw_exec_ftp.ksh Failed while FTP the ${ctl_fil_nme}.* control file"
     echo "***************************\nTO RECOVER: PLEASE FOLLOW THE STEPS UNDER [RECOVERY-6]in FTP_RECOVERY_STEPS Document\n"
     echo "RECOVERY DOCUMENT LINK: ${Recovery_link} \n***************************" 
     MSG="edw_exec_ftp.ksh Failed while FTP the ${ctl_fil_nme}.* control file "
     mailx -s "Error Message for src_sys_xtr_id=${src_sys_xtr_id} and sys_xtr_run_lod_dtm=${sys_xtr_run_lod_dtm}, from `hostname`: $PROGNAME.FTP Failed because edw_exec_ftp.ksh script Failed" $ADDRESSEES < $LOGFILE
     exit $RC
   else
     echo "Successfully executed edw_exec_ftp.ksh script for ${ctl_fil_nme}.* control file\n" 
   fi


awk '{print "\"P\",\""FILENAME "\"," $0}' dwhbook*.dat | more   
sed -i -e 's/,/|/g' test.csv