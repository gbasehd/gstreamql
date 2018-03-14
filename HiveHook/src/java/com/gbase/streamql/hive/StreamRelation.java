package com.gbase.streamql.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
            String query = "select begin,end,type,sql from relation";
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
        //return getPrevMock(name);
    }

    public String getSql(String name) throws Exception{
        if(!this.isInited){ init(); }
        return this.sql.get(this.id.get(name));
        //return getSqlMock(name);
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
        /*
        if(name.equals("o") ||
                name.equals("d1")||
                name.equals("d2")||
                name.equals("d3")||
                name.equals("d4")) {
            has = true;
        }
        */
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
        /*
        if(name.equals("o") ||
                name.equals("d1")||
                name.equals("d2")||
                name.equals("d3")||
                name.equals("d4")) {
            has = true;
        }*/
        return has;
        //"SELECT EXISTS(SELECT `begin` FROM `relation` WHERE `end` = \"" + name + "\")";
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
        /*
        if(name.equals("o")){
            is = true;
        }*/
        return is;
        //"SELECT EXISTS(SELECT * FROM `relation` WHERE `end` = \"" + name + "\")";
    }

    private String[] getPrevMock(String name)
    {
        ArrayList<String> prevs = new ArrayList<String>();
        if(name.equals("o")){
            prevs.add("d3");
            prevs.add("d4");
        }
        if(name.equals("d1")){
            prevs.add("i1");
            prevs.add("i2");
        }
        if(name.equals("d2")){
            prevs.add("i3");
        }
        if(name.equals("d3")){
            prevs.add("i3");
        }
        if(name.equals("d4")){
            prevs.add("d1");
            prevs.add("d2");
        }
        String[] result = new String[prevs.size()];
        result = prevs.toArray(result);
        return result;
    }

    private String getSqlMock(String name){
        String sql = "";
        if(name.equals("d1")){
            sql = "create stream d1 as select … from i1 join i2 …";
        }
        else if(name.equals("d2")){
            sql = "create stream d2 as select … from i3 …";
        }
        else if(name.equals("d3")){
            sql = "create stream d3 as select … from i3 …";
        }
        else if(name.equals("d4")){
            sql = "create stream d4 as select … from d1 join d2 …";
        }
        else if(name.equals("o")){
            sql = "insert into o select … from d3 join d4 …";
        }
        return sql;
    }
}
