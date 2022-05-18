#!/bin/bash


perem="$(hdfs dfs -count /user/hive/warehouse/pgw.db/pgw_out1/*.0. | awk '{print $2}')"

if [[ perem -eq 0 ]];then
    perem2="$(hdfs dfs -count /user/progr/local_out/error.log | awk '{print $2}')"
    if [[ perem2 -ne 0 ]];then
         hadoop jar /home/progr/export_sqoop_error/oracle.jar;
         hadoop fs -rm /user/progr/local_out/error.log;
         echo $(date +%d-%m-%Y\ %H:%M:%S) "Data not found">>export_data_error.log #1>&2;
    else
        echo $(date +%d-%m-%Y\ %H:%M:%S) "Given Path is empty">>export_oracle_err.log #1>&2;
    fi;
else    
    hadoop jar /home/progr/export_sqoop/oracle.jar;   
    hadoop fs -rm /user/hive/warehouse/pgw.db/pgw_out1/*.*;
    hadoop fs -rm /user/progr/local_out/success.log;
    echo $(date +%d-%m-%Y\ %H:%M:%S) "Given Path is not empty">>export_oracle.log #1>&2; 
fi;