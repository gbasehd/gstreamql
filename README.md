# gstreamql
demo 步骤：
1、环境准备
  sh flink-prepare.sh flink.json
  -- CREATE TABLE mjw.streamjobmgr(name string, pid string, jobid string, status string, define string) CLUSTERED BY (name) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
  -- truncate table mjw.streamjobmgr;
  
  -- 创建input topic
  /usr/ghd/current/kafka-broker/bin/kafka-topics.sh --zookeeper s1:2181,s2:2181,s3:2181 --create --topic yyj-input --partitions 3 --replication-factor 2
  -- 创建output topic
  /usr/ghd/current/kafka-broker/bin/kafka-topics.sh --zookeeper s1:2181,s2:2181,s3:2181 --create --topic yyj-output --partitions 3 --replication-factor 2
  -- 生产数据
  /usr/ghd/current/kafka-broker/bin/kafka-console-producer.sh --broker-list s1:6667,s2:6667,s3:6667 --topic yyj-input
  -- 消费数据
  /usr/ghd/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper s1:2181,s2:2181,s3:2181 --topic yyj-output --from-beginning
  
2、执行
  -- nc -l 9001
  CREATE STREAMJOB streamTest TBLPROPERTIES ("jobdef"="/streamingPro/flink.json");
  SHOW STREAMJOBS;
  START STREAMJOB streamTest; 
  http://192.167.1.222:8081/#/overview
  stop streamjob streamTest;
  drop streamjob streamTest;

注：需要将脚本放到/home/mjw下