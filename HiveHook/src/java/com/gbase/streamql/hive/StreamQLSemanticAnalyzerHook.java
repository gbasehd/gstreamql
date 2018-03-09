package com.gbase.streamql.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;
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

        if(astNode.getToken().getType() == HiveParser.TOK_QUERY
                && astNode.getChildCount() == 2
                && astNode.getChild(0).getType() == HiveParser.TOK_FROM
                && astNode.getChild(1).getType() == HiveParser.TOK_INSERT) {
            //simple insert select
            // eg. insert into a select b;
            StreamQLParser parser = new StreamQLParser(hiveSemanticAnalyzerHookContext.getCommand());
            parser.parse();
            switch (parser.getCmdType()) {
                case INSERT_STREAM: {
                    SessionState ss = SessionState.get();
                    Map<String, String> hiveVars = ss.getHiveVariables();

                    StringBuilder inputStreams = new StringBuilder();
                    StringBuilder outputStreams = new StringBuilder();
                    //simple insert select
                    // eg. insert into a select b;

                    //input
                    getTableList(astNode.getChild(0).toStringTree(), inputStreams);
                    getTableList(astNode.getChild(1).toStringTree(), outputStreams);

                    hiveVars.put("INPUT_STREAMS", inputStreams.toString());
                    hiveVars.put("OUTPUT_STREAMS", outputStreams.toString());
                    hiveVars.put("ORG_SQL", hiveSemanticAnalyzerHookContext.getCommand());
                    Utility.Logger("INPUT_STREAMS:" + inputStreams.toString());
                    Utility.Logger("OUTPUT_STREAMS:" + outputStreams.toString());
                    Utility.Logger("ORG_SQL:" + hiveVars.get("ORG_SQL"));

                }
            }
        }

        return astNode;
    }

    public void postAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, List<Task<? extends Serializable>> list) throws SemanticException {
        SessionState ss = SessionState.get();
        Map<String, String> hiveVars = ss.getHiveVariables();

        if(hiveVars.size() > 0) {
            String OUTPUT_STREAM = "output";
            String DERIVE_STREAM = "derive";
            if (hiveVars.keySet().contains("ORG_SQL")) {
                StreamQLParser parser = new StreamQLParser(hiveVars.get("ORG_SQL"));
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
                    //simple insert select
                    // eg. insert into a select b;

                    //input
                    edgeInfo.put(Utility.COL_BEGIN, hiveVars.get("INPUT_STREAMS"));
                    //output
                    edgeInfo.put(Utility.COL_END, hiveVars.get("OUTPUT_STREAMS"));

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
        }


    }

    /**
     * 递归截取字符串获取表名
     * @param strTree
     * @return
     */
    private void getTableList(String strTree, StringBuilder tabNames){
        int i1 = strTree.indexOf("tok_tabname");
        String substring1 = "";
        String substring2 = "";
        if(i1>0){
            substring1 = strTree.substring(i1+12);
            int i2 = substring1.indexOf(")");
            substring2 = substring1.substring(0,i2);
            System.out.println(substring2);
            tabNames.append(substring2).append(",");
            getTableList(substring1, tabNames);
        }
    }
}
