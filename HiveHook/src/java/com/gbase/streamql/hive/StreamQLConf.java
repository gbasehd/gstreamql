package com.gbase.streamql.hive;

public class StreamQLConf {
    private String dbName = "mjw";
    private String jsonFileDir = "/home/mjw";
    private boolean IS_DEBUG = false;
    private int MIN_WAITS_SECOND_INTERVAL = 1;
    private int MAX_TRY_TIMES = 50;
    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String url = "jdbc:hive2://192.167.1.222:10000/default";
    private String user = "hive";
    private String pass = "hive";

    public String getDbName(){
        return this.dbName;
    }
    public String getJsonFileDir(){
        return this.jsonFileDir;
    }
    public boolean isDebug(){
        return this.IS_DEBUG;
    }
    public int getMinWaitsSecondInterval(){
        return this.MIN_WAITS_SECOND_INTERVAL;
    }
    public int getMaxTryTimes(){
        return this.MAX_TRY_TIMES;
    }
    public String getDriverName(){
        return this.driverName;
    }
    public String getHiveUrl(){
        return this.url;
    }
    public String getHiveUser(){
        return this.user;
    }
    public String getHivePass(){
        return this.pass;
    }
}
