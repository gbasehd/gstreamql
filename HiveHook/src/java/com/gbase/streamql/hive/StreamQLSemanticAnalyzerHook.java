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
        Utility.Logger("preAnalyze: " + hiveSemanticAnalyzerHookContext.getCommand() + "\n");
        return astNode;
    }

    public void postAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, List<Task<? extends Serializable>> list) throws SemanticException {
        SessionState ss = SessionState.get();
        Map<String, String> hiveVars = ss.getHiveVariables();

        if(hiveVars.size() > 0 && hiveVars.keySet().contains("IS_STREAM_SQL")) {
            String OUTPUT_STREAM = "output";
            String DERIVE_STREAM = "derive";
            Utility.Logger("postAnalyze: " + hiveSemanticAnalyzerHookContext.getCommand());
            Map<String, String> edgeInfo = new HashMap<String, String>();
            //simple insert select
            // eg. insert into a select b;

            //input
            edgeInfo.put(Utility.COL_BEGIN, hiveVars.get("INPUT_STREAMS"));
            //output
            edgeInfo.put(Utility.COL_END, hiveVars.get("OUTPUT_STREAMS"));

            //runtimeType
            edgeInfo.put(Utility.COL_RUNTIME_TYPE, hiveVars.get("RUN_TIME_TYPE"));
            //sql
            edgeInfo.put(Utility.COL_SQL, hiveVars.get("SUB_SELECT_SQL"));
            try {
                Utility.edgePersist(edgeInfo);
                clearSession();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    private void clearSession() {
        SessionState ss = SessionState.get();
        Map<String, String> hiveVars = ss.getHiveVariables();
        hiveVars.remove("IS_STREAM_SQL");
        hiveVars.remove("INPUT_STREAMS");
        hiveVars.remove("OUTPUT_STREAMS");
        hiveVars.remove("RUN_TIME_TYPE");
        hiveVars.remove("ORG_SQL");
        hiveVars.remove("SUB_SELECT_SQL");
    }

}
