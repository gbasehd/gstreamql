
package com.gbase.streamql.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class HiveService {
    static Logger logger = Logger.getLogger(HiveService.class);
    public static String dirverName = "org.apache.hive.jdbc.HiveDriver";
    public static String url = "jdbc:hive2://192.167.1.223:10000/default";
    public static String user = "hive";
    public static String pass = "hive";

    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(dirverName);
            conn = DriverManager.getConnection(url, user, pass);
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