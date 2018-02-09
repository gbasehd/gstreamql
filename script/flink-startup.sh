#!/bin/bash
rm /home/mjw/streamingpro/output -f
rm /home/mjw/nohup.out -f
rm /home/mjw/flink-out.log -f
rm /home/mjw/flink-out.err -f
nohup /home/mjw/yyj/flink/flink-1.4.0-hadoop27-scala-211/bin/flink run  -c streaming.core.StreamingApp /home/mjw/yyj/streamingpro/streamingpro-flink-0.4.15-SNAPSHOT-1.4.0.jar -streaming.name $1 -streaming.platform flink_streaming -streaming.job.file.path $2 1> /home/mjw/flink-out.log 2>/home/mjw/flink-out.err &
