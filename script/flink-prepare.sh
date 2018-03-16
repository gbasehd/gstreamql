#!/bin/bash
hdfs dfs -rm -f "/streamingPro/$1"
hdfs dfs -put "$1" /streamingPro
rm /home/mjw/streamingpro/output -f
hdfs dfs -ls /streamingPro
