package com.gbase.streamql.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class StreamRelation {
    private ArrayList<String> begin = new ArrayList<String>();
    private ArrayList<String> end = new ArrayList<String>();
    private ArrayList<String> type = new ArrayList<String>(); // runtime-type
    private ArrayList<String> sql = new ArrayList<String>();
    private HashMap<String,Integer> id = new HashMap<String,Integer>(); // name->int
    private int count = 0;
    private boolean isInited = false;

    private void  init() throws  Exception {
        try{
            Connection conn = HiveService.getConn();
            Statement stmt  = HiveService.getStmt(conn);
            String query = "select source,dest,runtimeType,sql from " + Conf.SYS_DB + ".relation";
            ResultSet res   = stmt.executeQuery(query);
            while(res.next()){
                this.count ++;
                this.begin.add(res.getString(1));
                this.type.add(res.getString(3));
                this.sql.add(res.getString(4));
                String name = res.getString(2);
                this.end.add(name);
                this.id.put(name,new Integer(this.count-1));
            }
            HiveService.closeStmt(stmt);
            HiveService.closeConn(conn);
            if(this.isInited = false){this.isInited = true;}
        }
        catch(Exception e){
           throw new Exception("Failed to init StreamReltaion. " + e.getMessage());
        }
    }


    public String[] getPrev(String name) throws Exception{
        if(!this.isInited){ init(); }
        String[] prevs = this.begin.get(this.id.get(name)).split(",");
        return prevs;
    }

    public String getSql(String name) throws Exception{
        if(!this.isInited){ init(); }
        return this.sql.get(this.id.get(name));
    }

    public boolean hasSql(String name) throws Exception{
        boolean has = false;
        String sql = new String();
        try{
            if(!this.isInited){ init(); }
            if(this.id.containsKey(name)){
                sql = this.sql.get(this.id.get(name)) ;
                if(!sql.isEmpty()){
                    has = true;
                }
            }
        }
        catch(Exception e){
            throw new Exception("Failed to execute \"hasSql\". "+e.getMessage());
        }
        return has;
    }

    public boolean hasPrev(String name) throws Exception{
        boolean has = false;
        String prevs = new String();
        try{
            if(!this.isInited){ init(); }
            if(this.id.containsKey(name)){
                prevs = this.begin.get(this.id.get(name)) ;
                if(!prevs.isEmpty()){
                    has = true;
                }
            }
        }
        catch(Exception e){
            throw new Exception("Failed to execute \"hasPrev\". "+e.getMessage());
        }
        return has;
    }

    public boolean isOutput(String name) throws Exception{
        boolean is = false;
        String type = new String();
        try{
            if(!this.isInited){ init(); }
            if(this.id.containsKey(name)){
                type = this.type.get(this.id.get(name)) ;
                if(type.equals("output")){
                    is = true;
                }
            }
        }
        catch(Exception e){
            throw new Exception("Failed to execute \"isOutput\". "+e.getMessage());
        }
        return is;
    }

}
