package com.gbase.streamql.hive;

public class StreamJobMetaData {
    private String name;
    private String pid;
    private String jobid;
    private String status;
    private String define;

    public String getName(){
        return this.name;
    }

    public String getStatus(){
        return this.status;
    }
    public String getDefine(){
        return this.define;
    }
    public void setName(String name){
        this.name = name;
    }

    public void setStatus(String status){
        this.status = status;
    }
    public void setDefine(String define){
        this.define = define;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
}
