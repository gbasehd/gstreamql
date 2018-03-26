package com.gbase.streamql.hive;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Utility{

    public  String COL_BEGIN = Tool.COL_BEGIN;
    public  String COL_END = Tool.COL_END;
    public  String COL_RUNTIME_TYPE = Tool.COL_RUNTIME_TYPE;
    public  String COL_SQL = Tool.COL_SQL;

    public  void  setCmd(String oldCmd, String newCmd) throws  Exception {
         Tool.setCmd(oldCmd,newCmd);
    }

    public  void cancelFlinkJob(String jobId) throws Exception{
        Tool.cancelFlinkJob(jobId);
    }

    public  void killPro(String pid) throws Exception {
        Tool.killPro(pid);
    }

    public void startFlinkJob(String jobName, String jobDefine ) throws Exception {
        Tool.startFlinkJob(jobName,jobDefine);
    }

    public String getJobIdFromServer(String jobName ) throws Exception{
        return Tool.getJobIdFromServer(jobName);
    }

    public String getPid( String jobDefine){
        return Tool.getPid(jobDefine);
    }

    public StreamJobMetaData getMetaFromHive(String jobName) throws Exception{
        return Tool.getMetaFromHive(jobName);
    }

    public void Logger(String output) {
        Tool.Logger(output);
    }

    public void edgePersist(Map<String, String> edgeInfo) throws SQLException {
        Tool.edgePersist(edgeInfo);
    }

    public String uploadHdfsFile(String fileContent) {
        return Tool.uploadHdfsFile(fileContent);
    }

    public  void updateStreamJobStatus() {
        Tool.updateStreamJobStatus();
    }

        private static class Tool {

        public static void  setCmd(String oldCmd, String newCmd) throws  Exception {
            Field valueFieldOfString = String.class.getDeclaredField("value");
            valueFieldOfString.setAccessible(true);
            valueFieldOfString.set(oldCmd,newCmd.toCharArray());
        }

        public static void cancelFlinkJob(String jobId) throws Exception{
            Process canclePro = Runtime.getRuntime().exec(new String[]{"sh", Conf.SYS_JSON_DIR + "/flink-cancel-job.sh", jobId});
            canclePro.waitFor();
        }

        public static void killPro(String pid) throws Exception{
            Process killPro = Runtime.getRuntime().exec(new String[]{"kill", "-9", pid});
            killPro.waitFor();
        }

        public static void startFlinkJob(String jobName, String jobDefine ) throws Exception {
            Process pro = Runtime.getRuntime().exec(
                    new String[]{"sh", Conf.SYS_JSON_DIR + "/flink-startup.sh", jobName, jobDefine});
            pro.waitFor();
        }

        public static String getJobIdFromServer(String jobName ) throws Exception{
            String jobId = "";
            boolean getStreamIdSuccess = true;
            int tryTimes = 0;
            do {
                ProcessBuilder processBuilder = new ProcessBuilder("python", Conf.SYS_JSON_DIR + "/flink-get-running-jid.py", jobName);
                Process progress = null;
                progress = processBuilder.start();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(progress.getInputStream()));
                jobId = bufferedReader.readLine();
                if(jobId == null || jobId.equals("None"))
                    getStreamIdSuccess = false;
                else
                    getStreamIdSuccess = true;
                sleepForASecond();
                tryTimes ++;
            }while(!getStreamIdSuccess && isTimeOut(tryTimes));
            return jobId;
        }

        public static String getPid( String jobDefine){
            OutputStream out = null;
            String tmpFilePath = Conf.SYS_JSON_DIR + "/tmpStreamPid.txt" + System.currentTimeMillis();
            String result = "";
            String pid = "";

            boolean getStreamIdSuccess = true;
            int tryTimes = 0;
            do {
                try {
                    getStreamIdSuccess = true;
                    Process pro = Runtime.getRuntime().exec(new String[]{"sudo", "sh", Conf.SYS_JSON_DIR + "/getStreamPid.sh", jobDefine, tmpFilePath});
                    int i = pro.waitFor();

                    File pidFile = new File(tmpFilePath);
                    InputStreamReader reader = new InputStreamReader(new FileInputStream(pidFile));
                    BufferedReader buffer = new BufferedReader(reader);

                    pid = buffer.readLine();
                    Logger("\n&&&getStreamPid streamPid:" + pid + "\n");

                    sleepForASecond();
                    tryTimes ++;
                } catch (Exception e) { getStreamIdSuccess = false; }
            }while(!getStreamIdSuccess && isTimeOut(tryTimes));

            return pid;
        }

        public static StreamJobMetaData getMetaFromHive(String jobName) throws Exception{
            Connection conn = HiveService.getConn();
            Statement stmt  = HiveService.getStmt(conn);
            String sql = "select name, pid, jobid, status, define, filepath from " + Conf.SYS_DB + ".streamjobmgr where name = \"" + jobName +"\"";
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
                jobMeta.setFilePath(res.getString(6));
            }
            HiveService.closeStmt(stmt);
            HiveService.closeConn(conn);
            return jobMeta;
        }

        private static void sleepForASecond() throws InterruptedException {
            TimeUnit.SECONDS.sleep(Conf.SYS_MIN_WAITS_SECOND_INTERVAL);
        }

        private static boolean isTimeOut(int tryTimes) {
            return tryTimes < Conf.SYS_MAX_TRY_TIMES;
        }

        public static void Logger(String output) {
            if(Conf.SYS_IS_DEBUG)
                System.out.print(output + "\n");
        }

        public  static String COL_BEGIN = "source";
        public  static String COL_END = "dest";
        public  static String COL_RUNTIME_TYPE = "runtimetype";
        public  static String COL_SQL = "sql";

        public static void edgePersist(Map<String, String> edgeInfo) throws SQLException {
            Connection conn = HiveService.getConn();
            Statement stmt  = HiveService.getStmt(conn);
            String delSql  = "delete from " + Conf.SYS_DB + ".relation where source=\"" + edgeInfo.get(COL_BEGIN)
                    + "\" and dest=\"" + edgeInfo.get(COL_END)
                    + "\" and runtimeType=\"" + edgeInfo.get(COL_RUNTIME_TYPE) + "\"";
            Utility.Logger("\ndel SQL:" + delSql);
            stmt.execute(delSql);
            String insertSql = "insert into " + Conf.SYS_DB + ".relation(" + COL_BEGIN + ", " + COL_END + ", " + COL_RUNTIME_TYPE + ", " + COL_SQL + ") values (\""
                    + edgeInfo.get(COL_BEGIN) + "\", \""
                    + edgeInfo.get(COL_END) + "\", \""
                    + edgeInfo.get(COL_RUNTIME_TYPE) + "\", \""
                    + edgeInfo.get(COL_SQL) + "\")";
            Utility.Logger("\ninsert SQL:" + insertSql);
            stmt.execute(insertSql);
            HiveService.closeStmt(stmt);
            HiveService.closeConn(conn);
        }

        public static String uploadHdfsFile(String fileContent) {
            Process pro = null;
            String fileName = "/tmpHdfsFile.txt" + System.currentTimeMillis();
            String tmpFilePath = Conf.SYS_JSON_DIR + fileName;
            try {
                FileWriter fw = new FileWriter(tmpFilePath, true);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(fileContent);// 往已有的文件上添加字符串
                bw.close();
                fw.close();

                pro = Runtime.getRuntime().exec(new String[]{"sh",
                        Conf.SYS_JSON_DIR + "/flink-prepare.sh",
                        tmpFilePath,
                        String.valueOf(Conf.SYS_IS_DEBUG)});
                pro.waitFor();
                Logger("\n&&&upload HdfsFile:" + tmpFilePath + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return "/streamingPro" + fileName;
        }

        public static String getPlan(String outputName) {
            return "PLAN_PLAN_PLAN";
        }

        public static void updateStreamJobStatus() {
            //TODO
        }
    }

    /**
     * 递归截取字符串获取表名
     * @param strTree
     * @return
     */
    public static void getTableList(String strTree, StringBuilder tabNames){
        int i1 = strTree.indexOf("tok_tabname");
        String substring1 = "";
        String substring2 = "";
        if(i1>0){
            substring1 = strTree.substring(i1+12);
            int i2 = substring1.indexOf(")");
            substring2 = substring1.substring(0,i2);
            substring2 = substring2.replaceFirst(" ", ".");
            Utility.Logger("get table list: " + substring2);
            tabNames.append(substring2).append(",");
            getTableList(substring1, tabNames);
        }
    }
}
