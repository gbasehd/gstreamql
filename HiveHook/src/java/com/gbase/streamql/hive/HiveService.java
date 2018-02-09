
package com.gbase.streamql.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class HiveService {
    static Logger logger = Logger.getLogger(HiveService.class);
    private static StreamQLConf conf = new StreamQLConf();

    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(conf.getDriverName());
            conn = DriverManager.getConnection(conf.getHiveUrl(),conf.getHiveUser(),conf.getHivePass());
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    public static Statement getStmt(Connection conn) throws SQLException{
        logger.debug(conn);
        if(conn == null){
            logger.debug("this conn is null");
        }
        return conn.createStatement();
    }

    public static void closeConn(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void closeStmt(Statement stmt){
        try {
            stmt.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}