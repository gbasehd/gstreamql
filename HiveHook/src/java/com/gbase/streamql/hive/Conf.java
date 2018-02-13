package com.gbase.streamql.hive;

public class Conf {
    public class System{
        public String DB = "mjw";
        public String JSON_DIR = "/home/mjw";
        public boolean IS_DEBUG = false;
        public int MIN_WAITS_SECOND_INTERVAL = 1;
        public int MAX_TRY_TIMES = 50;
    }
    public class Hive{
        public String DRIVER = "org.apache.hive.jdbc.HiveDriver";
        public String URL = "jdbc:hive2://192.167.1.225:10000/default";
        public String USER = "hive";
        public String PASS = "hive";
    }
    public class Job{
        public ENGINE ENG = ENGINE.FLINK;
        public FS TARGET = FS.HDFS;
    }

    public static System SYS;
    public static Hive HIVE;
    public static Job JOB;

    public static void Init(){
       //TODO
       // read variables from config file and covering corresponding variables
    }
}
