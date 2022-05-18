#!/bin/bash

hadoop jar /home/progr/import_sqoop/oracle.jar;    
echo $(date +%d-%m-%Y\ %H:%M:%S)>>oracle.log #1>&2;

pid=$(ps ax | grep 'FCronDaemon' | grep -v grep | awk '{ print $1; }')

if [[ -n $pid ]]
then
  echo "stopping uniq_name_simple_daemon"
  kill -TERM $pid
else
  echo "nothing to stop"
fi;


pid1=$(ps ax | grep 'postdrop' | grep -v grep | awk '{ print $1; }')

if [[ -n $pid1 ]]
then
  echo "stopping uniq_name_simple_daemon"
  kill -TERM $pid1
else
  echo "nothing to stop"
fi;