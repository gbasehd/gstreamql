package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.session.SessionState;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class Utility {

    public static void  setCmd(String oldCmd, String newCmd) throws  Exception {
        Field valueFieldOfString = String.class.getDeclaredField("value");
        valueFieldOfString.setAccessible(true);
        valueFieldOfString.set(oldCmd,newCmd.toCharArray());
    }

    public static StreamJobMetaData getStreamJobMetaData(String jobName) throws Exception{
        Connection conn = HiveService.getConn();
        Statement stmt  = HiveService.getStmt(conn);
        String sql = "select name, pid, jobid, status, define from mjw.streamjobmgr where name = \"" + jobName +"\"";
        //System.out.println(sql);
        ResultSet res   = stmt.executeQuery(sql);
        ResultSetMetaData meta = res.getMetaData();

        StreamJobMetaData jobMeta = null;
        while(res.next()) {
            jobMeta = new StreamJobMetaData();
            jobMeta.setName(res.getString(1));
            jobMeta.setPid(res.getString(2));
            jobMeta.setJobid(res.getString(3));
            jobMeta.setStatus(res.getString(4));
            jobMeta.setDefine(res.getString(5));
        }
        HiveService.closeStmt(stmt);
        HiveService.closeConn(conn);

        return jobMeta;
    }
}
