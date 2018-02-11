package com.gbase.streamql.hive;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class StreamJob {
    private StreamQLConf conf;

    public StreamJob(StreamQLConf conf)
    {
        this.conf = conf;
    }
    public void stopStreamJob(StreamJobMetaData jobMetaData) throws Exception {
        //*********** cancle  jobflink
        Process canclePro = Runtime.getRuntime().exec(new String[]{"sh", conf.getJsonFileDir() + "/flink-cancel-job.sh", jobMetaData.getJobid()});
        canclePro.waitFor();

        //*********** kill pid
        Process killPro = Runtime.getRuntime().exec(new String[]{"kill", "-9", jobMetaData.getPid()});
        killPro.waitFor();
    }

    public void startStreamJob(ENGINE streamEngineType, FS jsonFilePath, StreamJobMetaData jobMetaData) throws Exception {
        switch(streamEngineType){
            case FLINK :
            {
                switch(jsonFilePath) {
                    case HDFS:
                        //exec cmd
                        Process pro = Runtime.getRuntime().exec(
                                new String[]{"sh", conf.getJsonFileDir() + "/flink-startup.sh", jobMetaData.getName(), jobMetaData.getDefine()});
                        pro.waitFor();
                        break;
                    case LOCAL:
                        break;
                    default:
                        break;
                }
                break;
            }
            default:
                break;
        }

    }

    public String getStreamJobId(String streamJobName) throws Exception {
        String jobId = "";
        boolean getStreamIdSuccess = true;
        int tryTimes = 0;
        do {
            ProcessBuilder processBuilder = new ProcessBuilder("python", conf.getJsonFileDir() + "/flink-get-running-jid.py", streamJobName);
            Process progress = null;
            progress = processBuilder.start();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(progress.getInputStream()));
            jobId = bufferedReader.readLine();
            if(jobId.equals("None"))
                getStreamIdSuccess = false;
            else
                getStreamIdSuccess = true;
            sleepForASecond();
            tryTimes ++;
        }while(!getStreamIdSuccess && isTimeOut(tryTimes));

        return jobId;
    }

    // tmp code
    public String getStreamPid(String jsonFilePath) throws InterruptedException {

        OutputStream out = null;
        String tmpFilePath = conf.getJsonFileDir() + "/tmpStreamPid.txt" + System.currentTimeMillis();
        String result = "";
        String pid = "";

        boolean getStreamIdSuccess = true;
        int tryTimes = 0;
        do {
            try {
                getStreamIdSuccess = true;
                Process pro = Runtime.getRuntime().exec(new String[]{"sudo", "sh", conf.getJsonFileDir() + "/getStreamPid.sh", jsonFilePath, tmpFilePath});
                pro.waitFor();

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
    private void sleepForASecond() throws InterruptedException {
        TimeUnit.SECONDS.sleep(conf.getMinWaitsSecondInterval());
    }

    private boolean isTimeOut(int tryTimes) {
        return tryTimes < conf.getMaxTryTimes();
    }

    private void Logger(String output) {
        if(conf.isDebug())
            System.out.print(output);
    }
}
