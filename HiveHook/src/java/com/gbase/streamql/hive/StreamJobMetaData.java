package com.gbase.streamql.hive;

public class StreamJobMetaData {
    private String name;
    private String id;
    private String status;
    private String define;

    public String getName(){
        return this.name;
    }
    public String getId(){
        return this.id;
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
    public void setId(String id){
        this.id = id;
    }
    public void setStatus(String status){
        this.status = status;
    }
    public void setDefine(String define){
        this.define = define;
    }
}
