package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamQLSemanticAnalyzerHook implements HiveSemanticAnalyzerHook {
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, ASTNode astNode) throws SemanticException {
        SessionState.getConsole().getOutStream().println("preAnalyze: " + hiveSemanticAnalyzerHookContext.getCommand() + "\n");
        //check is stream or not
        getTableList(astNode.toStringTree());

        return astNode;
    }

    public void postAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, List<Task<? extends Serializable>> list) throws SemanticException {
        String OUTPUT_STREAM = "output";
        String DERIVE_STREAM = "derive";

        StreamQLParser parser = new StreamQLParser(hiveSemanticAnalyzerHookContext.getCommand());
        parser.parse();
        String runtimeType = "";
        switch (parser.getCmdType()) {
            case INSERT_STREAM:
                runtimeType = OUTPUT_STREAM;
                break;
            case CREATE_AS_SELECT:
                runtimeType = DERIVE_STREAM;
                break;
            default:
                break;
        }
        if(!runtimeType.equals("")) {
            Utility.Logger("postAnalyze: " + hiveSemanticAnalyzerHookContext.getCommand());
            Map<String, String> edgeInfo = new HashMap<String, String>();
            //input
            StringBuilder inputStreams = new StringBuilder();
            for(ReadEntity e: hiveSemanticAnalyzerHookContext.getInputs()) {
                inputStreams.append(e.getName());
                inputStreams.append(",");
            }
            edgeInfo.put(Utility.COL_BEGIN, inputStreams.toString());
            //output
            StringBuilder outputStreams = new StringBuilder();
            for(WriteEntity e: hiveSemanticAnalyzerHookContext.getOutputs()) {
                outputStreams.append(e.getName());
                outputStreams.append(",");
            }
            edgeInfo.put(Utility.COL_END, outputStreams.toString());
            //runtimeType
            edgeInfo.put(Utility.COL_RUNTIME_TYPE, runtimeType);
            //sql
            edgeInfo.put(Utility.COL_SQL, parser.getTransformSql());
            try {
                Utility.edgePersist(edgeInfo);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 递归截取字符串获取表名
     * @param strTree
     * @return
     */
    private List<String> getTableList(String strTree){
        List<String> list = new ArrayList<String>();
        int i1 = strTree.indexOf("TOK_TABNAME");
        String substring1 = "";
        String substring2 = "";
        if(i1>0){
            substring1 = strTree.substring(i1+12);
            int i2 = substring1.indexOf(")");
            substring2 = substring1.substring(0,i2);
            System.out.println(substring2);
            list.add(substring2);
            getTableList(substring1);
        }
        return list;
    }
}
