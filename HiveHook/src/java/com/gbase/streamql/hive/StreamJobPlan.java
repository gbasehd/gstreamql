package com.gbase.streamql.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class StreamJobPlan {

    private String[] inputName;
    private String outputName;
    private String plan;

    public StreamJobPlan(String[] inputName, String outputName){
        this.inputName = inputName;
        this.outputName = outputName;
    }

    private ArrayList<String> getInputs(String current){
        ArrayList<String> inputs = new ArrayList<String>();
        StreamRelation relation = new StreamRelation();
        if(relation.hasPrev(current,RUNTIME_TYPE.DERIVE)) {
            String[] prevs = relation.getPrev(current, RUNTIME_TYPE.DERIVE);
            for(int i = 0; i < prevs.length; i++) {
                ArrayList<String> branchInputs = getInputs(prevs[i]);
                inputs.addAll(branchInputs);
            }
        }
        else {
            inputs.add(current);
        }

        return inputs;
    }


    public String getPlan() throws Exception {
        StreamRelation relation = new StreamRelation();

        if(relation.hasPrev(this.outputName,RUNTIME_TYPE.OUTPUT)) {
            ArrayList<String> allInputs = new ArrayList<String>();
            String[] preStreams = relation.getPrev(this.outputName,RUNTIME_TYPE.OUTPUT);
            for(int i = 0; i < preStreams.length; i++) {
                ArrayList<String> inputs = getInputs(preStreams[i]);
                allInputs.addAll(inputs);
            }

            //remove duplicate
            HashSet h = new HashSet(allInputs);
            allInputs.clear();
            allInputs.addAll(h);

            String[] allInputs2 = new String[allInputs.size()];
            allInputs2 = allInputs.toArray(allInputs2);
            Arrays.sort(allInputs2);
            Arrays.sort(inputName);
            if (Arrays.equals(allInputs2, inputName)) {

            }
            else {
                throw new Exception(String.format("The input stream \'%s\' cannot match", this.inputName));
            }
        }
        else {
            throw new Exception(String.format("The output stream \'%s\' does not exist", this.outputName));
        }
        return this.plan;
    }
}

