package com.gbase.streamql.hive;

public class Common {

}

enum CMD {
    CREATE_STREAMJOB,
    SHOW_STREAMJOBS,
    START_STREAMJOB,
    STOP_STREAMJOB,
    DROP_STREAMJOB,
    CREATE_STREAM,
    SHOW_STREAMS,
    DROP_STREAM,
    UNMATCHED
}

enum STATUS{
    RUNNING,
    STOPPED
}

enum ENGINE{
    FLINK,
    SPARK
}

enum FS{
    LOCAL,
    HDFS
}