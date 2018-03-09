package com.gbase.streamql.hive;

import java.util.ArrayList;

public class StreamRelation {

    public String[] getPrev(String name){
        return getPrevMock(name);
    }

    public String getSql(String name){
        return getSqlMock(name);
    }

    public boolean hasSql(String name){
        boolean has = false;
        if(name.equals("o") ||
                name.equals("d1")||
                name.equals("d2")||
                name.equals("d3")||
                name.equals("d4")) {
            has = true;
        }
        return has;
    }

    public boolean hasPrev(String name){
        boolean has = false;
        if(name.equals("o") ||
                name.equals("d1")||
                name.equals("d2")||
                name.equals("d3")||
                name.equals("d4")) {
            has = true;
        }
        return has;
    }

    public boolean isOutput(String name){
        boolean is = false;
        if(name.equals("o")){
            is = true;
        }
        return is;
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
