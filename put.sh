#!/bin/bash

perem=false

for file in /var/ftp/pub/*.*
  do
    if [ "$(ls -A /var/ftp/pub/)" ]; then 
      sleep 120;
      FILENAME=/var/ftp/pub/*.*
      SIZE=$(du -sb $FILENAME | awk '{ print $1 }')
      if ((SIZE >=92160000)); then    
        echo $(date +%d-%m-%Y\ %H:%M:%S) "Copy succes">>log3.log
        hadoop fs -put -f $file /user/hive/warehouse/pgw.db/pgw_in/

        if [ $? -eq 0 ]; then
           rm -f $file
           echo "Delete succes">>log3.log
           perem=true
        else
           echo $(date +%d-%m-%Y\ %H:%M:%S) "Delete Not succes">>log_error.log 
        fi 
      else
         echo "less"; 
      fi
      else
         echo $(date +%d-%m-%Y\ %H:%M:%S) "Copy error">>log_error.log #1>&2
      fi
  done 

if [ "$perem" = true ]; then 
     hadoop jar /home/progr/oracle_time/oracle.jar

fi;