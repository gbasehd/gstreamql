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
        String sql = "select * from mjw.streamjobmgr where name = \"" + jobName +"\"";
        System.out.println(sql);
        ResultSet res   = stmt.executeQuery(sql);
        ResultSetMetaData meta = res.getMetaData();

        StreamJobMetaData jobMeta = new StreamJobMetaData();
        while(res.next()) {
            jobMeta.setName(res.getString(1));
            jobMeta.setId(res.getString(2));
            jobMeta.setStatus(res.getString(3));
            jobMeta.setDefine(res.getString(4));
        }
        HiveService.closeStmt(stmt);
        HiveService.closeConn(conn);

        return jobMeta;
    }
}
