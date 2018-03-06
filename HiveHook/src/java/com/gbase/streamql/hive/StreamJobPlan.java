package com.gbase.streamql.hive;

import java.util.Arrays;
import java.util.HashSet;

public class StreamJobPlan {

    private String[] inputName;
    private String outputName;
    private String planContent;
    private int count;

    public StreamJobPlan(String[] inputName, String outputName){
        this.inputName = inputName;
        this.outputName = outputName;
        count = 0;
        planContent = "";
    }

    public String getContent() throws Exception {
        StreamRelation relation = new StreamRelation();
        String planContent = "";

        if(relation.isOutput(this.outputName)) {
            HashSet<String> allInputs = new HashSet<String>();
            allInputs = getLeaves(this.outputName);
            String[] allInputsArr = new String[allInputs.size()];
            allInputsArr = allInputs.toArray(allInputsArr);
            Arrays.sort(allInputsArr);
            Arrays.sort(inputName);
            if (Arrays.equals(allInputsArr, inputName)) {
                planContent = this.planContent;
            }
            else {
                throw new Exception(String.format("The input stream \'%s\' cannot match", getInputNames()));
            }
        }
        else {
            throw new Exception(String.format("The output stream \'%s\' does not exist", this.outputName));
        }
        return planContent;
    }

    private void addPlanContent(String name){
        this.planContent += "[" + this.count +"]";
        this.planContent += name;
        this.planContent += "->";
    }
    private void markeInput(String name){
        this.planContent += "[" + this.count +"]";
        this.planContent += name;
        this.planContent += "(end) ";
        this.count++;
    }

    private  String getInputNames()
    {
        String inputNames = "";
        for(int i = 0 ; i < this.inputName.length-1; i++){
            inputNames += this.inputName[i];
            inputNames += ", ";
        }
        inputNames += this.inputName[this.inputName.length-1];
        return inputNames;
    }

    private HashSet<String> getLeaves(String root){
        HashSet<String> leaves = new HashSet<String>();
        StreamRelation relation = new StreamRelation();
        if(relation.hasPrev(root)) {
            addPlanContent(root);
            String[] prevs = relation.getPrev(root);
            for(int i = 0; i < prevs.length; i++) {
                HashSet<String> branchLeaves = getLeaves(prevs[i]);
                leaves.addAll(branchLeaves);
            }
        }
        else {
            leaves.add(root);
            markeInput(root);
        }
        return leaves;
    }
}

