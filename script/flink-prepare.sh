#!/bin/bash
hdfs dfs -rm -f "/streamingPro/$1"
hdfs dfs -put "$1" /streamingPro
if [ $2 = "false" ]
then
    rm $1 -f
fi
hdfs dfs -ls /streamingPro
