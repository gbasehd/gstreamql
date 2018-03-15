package com.gbase.streamql.hive;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Utility {

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
        String sql = "select name, pid, jobid, status, define from " + Conf.SYS_DB + ".streamjobmgr where name = \"" + jobName +"\"";
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

    public static String COL_BEGIN = "source";
    public static String COL_END = "dest";
    public static String COL_RUNTIME_TYPE = "runtimetype";
    public static String COL_SQL = "sql";

    public static void edgePersist(Map<String, String> edgeInfo) throws SQLException {
        Connection conn = HiveService.getConn();
        Statement stmt  = HiveService.getStmt(conn);
        String sql = "insert into " + Conf.SYS_DB + ".relation(" + COL_BEGIN + ", " + COL_END + ", " + COL_RUNTIME_TYPE + ", " + COL_SQL + ") values (\""
        //String sql = "insert into tmp(" + COL_BEGIN + ", " + COL_END + ", " + COL_RUNTIME_TYPE + ", " + COL_SQL + ") values (\""
                + edgeInfo.get(COL_BEGIN) + "\", \""
                + edgeInfo.get(COL_END) + "\", \""
                + edgeInfo.get(COL_RUNTIME_TYPE) + "\", \""
                + edgeInfo.get(COL_SQL) + "\")";
        stmt.execute(sql);
        //stmt.execute("show tables");
        Utility.Logger("\nSQL:" + sql);
        HiveService.closeStmt(stmt);
        HiveService.closeConn(conn);
    }

    public static String uploadHdfsFile(String fileContent) {
        Process pro = null;
        String tmpFilePath = Conf.SYS_JSON_DIR + "/tmpHdfsFile.txt" + System.currentTimeMillis();
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

        return tmpFilePath;
    }

    public static String getPlan(String outputName) {
        return "PLAN_PLAN_PLAN";
    }

}
