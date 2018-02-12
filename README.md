demo 步骤：
# 编码后
  cp /home/mjw/gstreamql/HiveHook/target/streamql-hive-hook-1.0-SNAPSHOT.jar /usr/ghd/current/hive-client/lib/
  -- hive.exec.driver.run.hooks=com.gbase.streamql.hive.StreamQLDriverRunHook

# 环境准备
  sh flink-prepare.sh flink.json(前提：user需要有操作hdfs权限，/streamingPro需要存在)
  -- python get-pip.py
  -- python -m pip install requests
  -- create database mjw;
  -- CREATE TABLE mjw.streamjobmgr(name string, pid string, jobid string, status string, define string) CLUSTERED BY (name) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
  -- truncate table mjw.streamjobmgr;
  
  -- 创建input topic
  /usr/ghd/current/kafka-broker/bin/kafka-topics.sh --zookeeper c1:2181,c2:2181,c3:2181 --create --topic yyj-input --partitions 3 --replication-factor 2
  -- 创建output topic
  /usr/ghd/current/kafka-broker/bin/kafka-topics.sh --zookeeper c1:2181,c2:2181,c3:2181 --create --topic yyj-output --partitions 3 --replication-factor 2
  -- 生产数据
  /usr/ghd/current/kafka-broker/bin/kafka-console-producer.sh --broker-list c1:6667,c2:6667,c3:6667 --topic yyj-input
  -- 消费数据
  /usr/ghd/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper c1:2181,c2:2181,c3:2181 --topic yyj-output --from-beginning
  
# 执行
  -- nc -l 9001
  CREATE STREAMJOB streamTest TBLPROPERTIES ("jobdef"="/streamingPro/flink.json");
  SHOW STREAMJOBS;
  START STREAMJOB streamTest; 
  http://192.167.1.222:8081/#/overview
  stop streamjob streamTest;
  drop streamjob streamTest;
  
注：需要将脚本放到StreamQLConf的jsonFileDir下