package com.gbase.streamql.hive;

import java.util.ArrayList;

public class StreamRelation {

    public String[] getPrev(String name, RUNTIME_TYPE type){
        return getPrevMock(name,type);
    }

    public boolean hasPrev(String name, RUNTIME_TYPE type){
        boolean has = false;
        if(type == RUNTIME_TYPE.OUTPUT){
            if(name.equals("o")){
               has = true;
            }
        }
        if(type == RUNTIME_TYPE.DERIVE){
            if(name.equals("d1") ||
                    name.equals("d2")||
                    name.equals("d3")||
                    name.equals("d4")) {
               has = true;
            }
        }
        return has;
    }

    private String[] getPrevMock(String name, RUNTIME_TYPE type)
    {
        ArrayList<String> prevs = new ArrayList<String>();
        if(type == RUNTIME_TYPE.OUTPUT){
            if(name.equals("o")){
                prevs.add("d3");
                prevs.add("d4");
            }
        }
        if(type == RUNTIME_TYPE.DERIVE){
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
        }
        String[] result = new String[prevs.size()];
        result = prevs.toArray(result);
        return result;
    }
}
