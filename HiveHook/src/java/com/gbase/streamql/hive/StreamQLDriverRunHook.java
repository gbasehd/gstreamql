/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.apache.hadoop.hive.ql.hooks;
package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamQLDriverRunHook implements HiveDriverRunHook {

    //def cmd type
    private final int CREATE_STREAMJOB = 6;
    private final int SHOW_STREAMJOBS = 1;
    private final int START_STREAMJOB = 2;
    private final int STOP_STREAMJOB = 3;
    private final int DROP_STREAMJOB = 4;
    private final int DESCRIBE_STREAMJOB = 5;
    private final int UNMATCHED = 0;

    //def match pattern
    private String PATTERN_CREATE_STREAMJOB = "^([ ]*CREATE[ ]+STREAMJOB[ ]+)([a-zA-Z0-9]+)([ ]+TBLPROPERTIES[ ]*\\(\\\"jobdef\\\"=\\\")([a-zA-Z0-9/\\.]+)(\\\"\\)[ ]*)$";
    private String PATTERN_SHOW_STREAMJOBS = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
    private String PATTERN_START_STREAMJOB = "(^[ ]*start[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_STOP_STREAMJOB = "(^[ ]*stop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_DROP_STREAMJOB = "(^[ ]*drop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_DESCRIBE_STREAMJOB = "^([ ]*describe[ ]+)([a-zA-Z/0-9]+)([ ]*)$";

    //def stream job status
    private String STATUS_RUNNING = "RUNNING";
    private String STATUS_STOPPED = "STOPPED";

    //def stream engine
    private static final int STREAM_FLINK_ENGINE = 0;
    private int STREAM_ENGINE_TYPE = STREAM_FLINK_ENGINE;

    //def json file path: hdfs; local file
    private final int SAVE_TO_FILE = 0;
    private final int SAVE_TO_HDFS = 1;

    //def cmd params
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_JOB_DEF = "streamJobDef";

    //store cmd params
    private Map<String, String> mapCmdParams = new HashMap<String, String>();

    //tmp param
    private String dbName = "mjw";
    private String jsonFileDir = "/home/mjw";

    private boolean IS_DEBUG = false;

    private int MIN_WAITS_SECOND_INTERVAL = 1;
    private final int MAX_TRY_TIMES = 50;

    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        Logger("change "+ hookContext.getCommand());
        String cmd = hookContext.getCommand();
        String myCmd = "";
        switch(getCmdType(cmd)) {
            case CREATE_STREAMJOB: {
                //check if streamjob name exists
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                if(jobMetaData != null && jobMetaData.getName().equals(getCmdParam(STREAM_JOB_NAME))) {
                    throw  new Exception("Create stream job error! Stream job name \"" + getCmdParam(STREAM_JOB_NAME) + "\" exists!");
                }
                //insert data
                myCmd = "Insert into " + dbName + ".streamjobmgr(name, pid, jobid, status, define) values ('" + getCmdParam(STREAM_JOB_NAME) + "',NULL,NULL,'STOPPED','" + getCmdParam(STREAM_JOB_DEF) + "')";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case SHOW_STREAMJOBS: {
                //TODO
                //check status again

                myCmd = "Select name, jobid, status, define from " + dbName + ".streamjobmgr";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case START_STREAMJOB: {
                //1.Select define,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Start stream job failed! create stream job \"" + getCmdParam(STREAM_JOB_NAME) + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS_STOPPED)) {
                    //3.exec stremingPro
                    startStreamJob(STREAM_ENGINE_TYPE, SAVE_TO_HDFS, jobMetaData);
                    //4.Update ghd.streamjobmgr set status = 'RUNNING' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + dbName +".streamjobmgr set status = '" + STATUS_RUNNING +
                            "' , pid = \"" + getStreamPid(jobMetaData.getDefine()) +
                            "\", jobid = \"" + getStreamJobId(getCmdParam(STREAM_JOB_NAME)) +
                            "\" where  name = \"" + getCmdParam(STREAM_JOB_NAME) + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    throw new Exception("Execute error! target stream job is running!");
                }
                break;
            }
            case STOP_STREAMJOB: {
                //1.Select id,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Stop stream job failed! create stream job \"" + getCmdParam(STREAM_JOB_NAME) + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS_RUNNING)) {
                    //3.Kill streamjob_id
                    stopStreamJob(jobMetaData);
                    //4.Update ghd.streamjobmgr set status = 'STOPPED' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + dbName +".streamjobmgr set status = '" + STATUS_STOPPED +
                            "' , pid = \"NULL\", jobid = \"NULL\" where  name = \"" + getCmdParam(STREAM_JOB_NAME) + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    //do nothing
                }
                break;
            }
            case DROP_STREAMJOB: {
                //1.Select status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Drop stream job failed! create stream job \"" + getCmdParam(STREAM_JOB_NAME) + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS_STOPPED)) {
                    //3.Delete from ghd.streamjobmgr where name = <streamjob_name>
                    myCmd = "Delete from " + dbName +".streamjobmgr where  name = \"" + getCmdParam(STREAM_JOB_NAME) + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    throw new Exception("Execute error! Unable to delete the running job!");
                }
                break;
            }
            /*case DESCRIBE_STREAMJOB: {
                myCmd = "desc table" + getCmdParam(STREAM_JOB_NAME);
                Utility.setCmd(cmd, myCmd);
                break;
            }*/
            case UNMATCHED:
                break;
            default:
                break;
        }

        Logger("to " + hookContext.getCommand());
    }

    private void stopStreamJob(StreamJobMetaData jobMetaData) throws Exception {
        //*********** cancle  jobflink
        Process canclePro = Runtime.getRuntime().exec(new String[]{"sh", jsonFileDir + "/flink-cancel-job.sh", jobMetaData.getJobid()});
        canclePro.waitFor();

        //*********** kill pid
        Process killPro = Runtime.getRuntime().exec(new String[]{"kill", "-9", jobMetaData.getPid()});
        killPro.waitFor();
    }

    private void startStreamJob(int streamEngineType, int jsonFilePath, StreamJobMetaData jobMetaData) throws Exception {
        switch(streamEngineType){
            case STREAM_FLINK_ENGINE :
            {
                switch(jsonFilePath) {
                    case SAVE_TO_HDFS:
                        //exec cmd
                        Process pro = Runtime.getRuntime().exec(
                                new String[]{"sh", jsonFileDir + "/flink-startup.sh", jobMetaData.getName(), jobMetaData.getDefine()});
                        pro.waitFor();
                        break;
                    case SAVE_TO_FILE:
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

    private String getStreamJobId(String streamJobName) throws Exception {
        String jobId = "";
        boolean getStreamIdSuccess = true;
        int tryTimes = 0;
        do {
            ProcessBuilder processBuilder = new ProcessBuilder("python", jsonFileDir + "/flink-get-running-jid.py", streamJobName);
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
    private String getStreamPid(String jsonFilePath) throws InterruptedException {

		OutputStream out = null;
        String tmpFilePath = jsonFileDir + "/tmpStreamPid.txt" + System.currentTimeMillis();
        String result = "";
        String pid = "";

        boolean getStreamIdSuccess = true;
        int tryTimes = 0;
        do {
            try {
                getStreamIdSuccess = true;
                Process pro = Runtime.getRuntime().exec(new String[]{"sudo", "sh", jsonFileDir + "/getStreamPid.sh", jsonFilePath, tmpFilePath});
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
        TimeUnit.SECONDS.sleep(MIN_WAITS_SECOND_INTERVAL);
    }

    private boolean isTimeOut(int tryTimes) {
        return tryTimes < MAX_TRY_TIMES;
    }

    private String getCmdParam(String keyName) {
        return mapCmdParams.get(keyName);
    }

    private int getCmdType(String cmd) {
        //init cmd type
        int cmdType = CREATE_STREAMJOB;
        switch(cmdType) {
            case CREATE_STREAMJOB: {
                String regExCreateStream = PATTERN_CREATE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if(matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    mapCmdParams.put(STREAM_JOB_DEF, matcher.group(4));
                    break;
                } else {
                    cmdType = SHOW_STREAMJOBS;
                }
            }
            case SHOW_STREAMJOBS: {
                String regExShowStream = PATTERN_SHOW_STREAMJOBS;
                Pattern pattern = Pattern.compile(regExShowStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    break;
                } else {
                    cmdType = START_STREAMJOB;
                }
            }
            case START_STREAMJOB: {
                String regExStartStream = PATTERN_START_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStartStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = STOP_STREAMJOB;
                }
            }
            case STOP_STREAMJOB: {
                String regExStopStream = PATTERN_STOP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStopStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = DROP_STREAMJOB;
                }
            }
            case DROP_STREAMJOB:{
                String regExDropStream = PATTERN_DROP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDropStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = DROP_STREAMJOB;
                }
            }
            /*case DESCRIBE_STREAMJOB: {
                String regExDescStream = PATTERN_DESCRIBE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDescStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } //else //do nothing
            }*/

            default: {
                cmdType = UNMATCHED;
                break;
            }
        }

        return cmdType;
    }

    //@Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        // do nothing
    }

    private void Logger(String output) {
        if(IS_DEBUG)
            System.out.print(output);
    }

}
