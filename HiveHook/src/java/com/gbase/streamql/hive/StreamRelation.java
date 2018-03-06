package com.gbase.streamql.hive;

import java.util.ArrayList;

public class StreamRelation {

    public String[] getPrev(String name){
        return getPrevMock(name);
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
}
