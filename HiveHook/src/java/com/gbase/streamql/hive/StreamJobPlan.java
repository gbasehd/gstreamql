package com.gbase.streamql.hive;

import java.util.*;

public class StreamJobPlan {

    private String[] inputNames;
    private String outputName;
    private String plan;
    private Stack<String> jsonStrStack = new Stack<String>() ; // Save the generated json string according to plan
    private int count;
    private StreamRelation relation;
    private boolean isGenerated = false;

    public StreamJobPlan(String[] inputNames, String outputName){
        this.inputNames = inputNames;
        this.outputName = outputName;
        this.count = 0;
        this.plan = "";
        this.relation = new StreamRelation();
    }

    public StreamJobPlan(StreamRelation r,String[] inputNames, String outputName){
        this.inputNames = inputNames;
        this.outputName = outputName;
        this.count = 0;
        this.plan = "";
        this.relation = r;
    }

    public String getJson() throws Exception{
        String jsonStr = new String();
        if(this.isGenerated) {
            jsonStr = getJsonHead();
            jsonStr += getJsonInput(this.inputNames);
            jsonStr += getJsonSqlAndOutput();
            jsonStr += getJsonTail();
        }
        else{
            throw new Exception(String.format("Must first execute function StreamJobPlan.generate() to get json string"));
        }
        return jsonStr;
    }

    public String print() throws Exception{
        String str = new String();
        if(!this.plan.isEmpty()){
            str = this.plan;
        }
        else{
            throw new Exception(String.format("Must first execute function StreamJobPlan.generate() to get plan content"));
        }
        return this.plan;
    }

    public void generate() throws Exception {
        if(this.outputName.isEmpty()){
            throw new Exception(String.format("Output cannot be empty!"));
        }
        if(this.inputNames.length == 0){
            throw new Exception(String.format("Input cannot be empty!"));
        }
        if(this.relation.isOutput(this.outputName)) {
            HashSet<String> realInputNames =  getLeaves(this.outputName); //Traversing plan while generating json string
            if(equals(this.inputNames,realInputNames)){
                if(!this.isGenerated) this.isGenerated = true;
            }
            else{
                throw new Exception(String.format("The input stream \'%s\' cannot match", getInputNames()));
            }
        }
        else {
            throw new Exception(String.format("The output stream \'%s\' does not exist", this.outputName));
        }
    }

    // Depth-first traversal algorithm
    private HashSet<String> getLeaves (String root) throws Exception{
        HashSet<String> leaves = new HashSet<String>();
        if(this.relation.hasPrev(root)) {
            savePlan(root);
            addContent(root);
            String[] prevs = this.relation.getPrev(root);
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

    private boolean equals(String[] source , HashSet<String> target ){
        String[] targetArr = new String[target.size()];
        targetArr = target.toArray(targetArr);
        Arrays.sort(source);
        Arrays.sort(targetArr);
        boolean isEqual = false;
        if(Arrays.equals(source,targetArr)){
           isEqual = true;
        }
        return isEqual;
    }


    private void savePlan(String name){
        this.plan += "[" + this.count +"]";
        this.plan += name;
        this.plan += "->";
    }

    private void addContent(String name) throws Exception {
        if(this.relation.isOutput(name)) {
            this.jsonStrStack.push(getJsonOutputTail());
            this.jsonStrStack.push(formatJsonOutput(name));
            this.jsonStrStack.push(getJsonOutputHead());
            this.jsonStrStack.push(formatJsonSql(name));
        }
        else{
            if(this.relation.hasSql(name)){
                this.jsonStrStack.push(formatJsonSql(name));
            }
        }
    }

    private  String getJsonInput(String[] inputNames){
        String jsonInput = getJsonInputHead();
        for(int i = inputNames.length-1; i >= 0; i--){
            jsonInput += formatJsonInput(inputNames[i],i);
        }
        jsonInput += getJsonInputTail();
        return jsonInput;
    }

    private String getJsonInputHead(){
        String head = new String();
        int base = 2;
        head = repeat("\t",base) + "{\r\n";
        head += repeat("\t",base+1) + "\"name\": \"flink.sources\",\r\n";
        head += repeat("\t",base+1) + "\"params\": [\r\n";
        return head;
    }

    private String formatJsonInput(String name, int count){
        String jsonFormatStr = new String();
        int base =3;
        jsonFormatStr = repeat("\t",base) + "{\r\n";
        jsonFormatStr += repeat("\t",base+1) + "\"format\": \"kafka\",\r\n";
        jsonFormatStr += repeat("\t",base+1) + "\"bootstrap.servers\": \"c1:6667,c2:6667,c3:6667\",\r\n";
        jsonFormatStr += repeat("\t",base+1) + "\"topics\": \"yyj-input\",\r\n";
        jsonFormatStr += repeat("\t",base+1) + "\"outputTable\": \"" + name + "\"\r\n";
        if(count > 0 ) {
            jsonFormatStr += repeat("\t",base) + "},\r\n";
        }
        else{
            jsonFormatStr += repeat("\t",base) + "}\r\n";
        }
        return jsonFormatStr;
    }

    private String getJsonInputTail(){
        String tail = new String();
        int base = 2;
        tail += repeat("\t",base+1) + "]\r\n";
        tail += repeat("\t",base) + "},\r\n";
        return tail;
    }

    private String getJsonSqlAndOutput(){
        String jsonSqlAndOutput = new String();
        Stack<String> tmp = (Stack<String>)this.jsonStrStack.clone();
        while(!tmp.empty()){
            jsonSqlAndOutput += tmp.pop() + "\r\n";
        }
        return jsonSqlAndOutput ;
    }

    private String formatJsonSql(String name) throws Exception{
        String jsonFormatStr = new String();
        int base = 2;
        jsonFormatStr = repeat("\t", base) + "{\r\n";
        jsonFormatStr +=repeat("\t", base+1) + "\"name\": \"flink.sql\",\r\n";
        jsonFormatStr +=repeat("\t", base+1) +  "\"params\": [\r\n";
        jsonFormatStr +=repeat("\t", base+1) + "{\r\n";
        jsonFormatStr +=repeat("\t", base+2) + "\"sql\": \"" + this.relation.getSql(name) + "\",\r\n";
        jsonFormatStr +=repeat("\t", base+2) + "\"outputTableName\": \"" + name + "\"\r\n";
        jsonFormatStr +=repeat("\t", base+1) + "}\r\n";
        jsonFormatStr +=repeat("\t", base+1) + "]\r\n";
        jsonFormatStr +=repeat("\t", base) + "},\r";
        return jsonFormatStr;
    }

    private String getJsonOutputHead() {
        String head = new String();
        int base=2;
        head = repeat("\t",base) + "{\r\n";
        head += repeat("\t",base+1) +  "\"name\": \"flink.outputs\",\r\n";
        head += repeat("\t",base+1) + "\"params\": [\r";
        return head;
    }

    private String formatJsonOutput(String name){
        String jsonFormatStr = new String();
        int base = 3;
        jsonFormatStr += repeat("\t", base) + "{\r\n";
        jsonFormatStr += repeat("\t", base+1) + "\"name\": \"jack\",\r\n";
        jsonFormatStr += repeat("\t", base+1) + "\"format\": \"kafka\",\r\n";
        jsonFormatStr += repeat("\t", base+1) + "\"topic\": \"yyj-output\",\r\n";
        jsonFormatStr += repeat("\t", base+1) + "\"bootstrap.servers\": \"c1:6667,c2:6667,c3:6667\",\r\n";
        jsonFormatStr += repeat("\t", base+1) + "\"inputTableName\": \""+name+"\"\r\n";
        jsonFormatStr += repeat("\t", base) + "}\r";
        return jsonFormatStr;
    }

    private String getJsonOutputTail() {
        String tail = new String();
        int base = 2;
        tail = repeat("\t",base+1) + "]\r\n";
        tail += repeat("\t",base) + "}\r";
        return tail;
    }

    private String repeat(String str, int count){
        String repeated = "";
        for(int i = 0; i < count; i++){
           repeated += str;
        }
        return repeated;
    }

    private String getJsonHead(){
        String head = new String();
        int base = 0;
        head = repeat("\t",base) + "{\r\n";
        head += repeat("\t",base+1) + "\"flink-example\": {\r\n";
        head += repeat("\t",base+2) + "\"desc\": \"demo\",\r\n";
        head += repeat("\t",base+2) + "\"strategy\": \"flink\",\r\n";
        head += repeat("\t",base+2) + "\"algorithm\": [],\r\n";
        head += repeat("\t",base+2) + "\"ref\": [],\r\n";
        head += repeat("\t",base+2) + "\"compositor\": [\r\n";
        return head;
    }

    private  String getJsonTail(){
        String tail = new String();
        int base = 0;
        tail = repeat("\t",base+2) + "],\r\n";
        tail += repeat("\t",base+2) + "\"configParams\": {\r\n";
        tail += repeat("\t",base+2) + "}\r\n";
        tail += repeat("\t",base+1) + "}\r\n";
        tail += repeat("\t",base) + "}\r";
        return tail;
    }

    private void markeInput(String name){
        this.plan += "[" + this.count +"]";
        this.plan += name;
        this.plan += "(end) ";
        this.count++;
    }

    private  String getInputNames()
    {
        String inputNames = "";
        for(int i = 0 ; i < this.inputNames.length-1; i++){
            inputNames += this.inputNames[i];
            inputNames += ", ";
        }
        inputNames += this.inputNames[this.inputNames.length-1];
        return inputNames;
    }

}

