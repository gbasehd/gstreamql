package com.gbase.streamql.hive;

public class Conf {
    //public class System{
        public static String SYS_DB = "mjw";
        public static String SYS_JSON_DIR = "/home/mjw";
        public static boolean SYS_IS_DEBUG = false;
        public static int SYS_MIN_WAITS_SECOND_INTERVAL = 1;
        public static int SYS_MAX_TRY_TIMES = 50;
    //}
    //public class Hive{
        public static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        public static String HIVE_URL = "jdbc:hive2://192.167.1.225:10000/default";
        public static String HIVE_USER = "hive";
        public static String HIVE_PASS = "hive";
    //}
    //public class Job{
        public static ENGINE JOB_ENG = ENGINE.FLINK;
        public static FS JOB_TARGET = FS.HDFS;
    //}

    //public System SYS = new System();
    //public static Hive HIVE;
    //public static Job JOB;

    public static void Init(){
       //TODO
       // read variables from config file and covering corresponding variables
    }
}
