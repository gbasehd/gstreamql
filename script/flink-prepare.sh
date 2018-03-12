#!/bin/bash
hdfs dfs -rm -f "/streamingPro/$1"
hdfs dfs -put "$1" /streamingPro
rm $1 -f
hdfs dfs -ls /streamingPro
