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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamQLDriverRunHook implements HiveDriverRunHook {

    //def cmd type
    private final int CREATE_STREAMJOB = 0;
    private final int SHOW_STREAMJOBS = 1;
    private final int START_STREAMJOB = 2;
    private final int STOP_STREAMJOB = 3;
    private final int DROP_STREAMJOB = 4;
    private final int DESCRIBE_STREAMJOB = 5;

    //def stream job status
    private String STATUS_RUNNING = "RUNNING";
    private String STATUS_STOPPED = "STOPPED";

    //def stream engine
    private static final int STREAM_FLINK_ENGINE = 0;
    private int STREAM_ENGINE_TYPE = STREAM_FLINK_ENGINE;

    //def json file path: hdfs; local file
    private static final int SAVE_TO_FILE = 0;
    private static final int SAVE_TO_HDFS = 1;

    //def cmd params
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_JOB_DEF = "streamJobDef";

    //store cmd params
    private Map<String, String> mapCmdParams = new HashMap<String, String>();

    //tmp param
    private String dbName = "mjw";
    private String jsonFileDir = "/home/mjw";

    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        SessionState.getConsole().getOutStream().println("change "+ hookContext.getCommand());
        String cmd = hookContext.getCommand();



        String myCmd = "";
        switch(getCmdType(cmd)) {
            case CREATE_STREAMJOB: {
                myCmd = "Insert into " + dbName + ".streamjobmgr values ('" + getCmdParam(STREAM_JOB_NAME) + "',NULL,'STOPPED','" + getCmdParam(STREAM_JOB_DEF) + "')";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case SHOW_STREAMJOBS: {
                myCmd = "Select * from " + dbName + ".streamjobmgr";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case START_STREAMJOB: {
                //1.Select define,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData.getStatus().equals(STATUS_STOPPED)) {
                    //3.exec stremingPro
                    startStreamJob(STREAM_ENGINE_TYPE, SAVE_TO_HDFS, jobMetaData.getDefine());
                    //4.Update ghd.streamjobmgr set status = 'RUNNING' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + dbName +".streamjobmgr set status = '" + STATUS_RUNNING + "' , id = "
                            + getStreamPid(jobMetaData.getDefine()) + " where  name = " + getCmdParam(STREAM_JOB_NAME);
                    Utility.setCmd(cmd, myCmd);
                } else {
                    //TODO
                }
                break;
            }
            case STOP_STREAMJOB: {
                //1.Select id,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData.getStatus().equals(STATUS_RUNNING)) {
                    //3.Kill streamjob_id
                    stopStreamJob(jobMetaData.getId());
                    //4.Update ghd.streamjobmgr set status = 'STOPPED' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + dbName +".streamjobmgr set status = '" + STATUS_STOPPED + "' , id = "
                            + getStreamPid(jobMetaData.getDefine()) + " where  name = " + getCmdParam(STREAM_JOB_NAME);
                    Utility.setCmd(cmd, myCmd);
                } else {
                    //TODO
                }
                break;
            }
            case DROP_STREAMJOB: {
                //1.Select status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(getCmdParam(STREAM_JOB_NAME));
                //2.check status
                if(jobMetaData.getStatus().equals(STATUS_STOPPED)) {
                    //3.Delete from ghd.streamjobmgr where name = <streamjob_name>
                    myCmd = "Delete from " + dbName +".streamjobmgr where  name = " + getCmdParam(STREAM_JOB_NAME);
                    Utility.setCmd(cmd, myCmd);
                } else {
                    //TODO
                }
                break;
            }
            /*case DESCRIBE_STREAMJOB: {
                myCmd = "desc table" + getCmdParam(STREAM_JOB_NAME);
                Utility.setCmd(cmd, myCmd);
                break;
            }*/
            default:
                break;
        }



        SessionState.getConsole().getOutStream().println("to " + hookContext.getCommand());
    }

    private void stopStreamJob(String id) {
        //generate cmd
        String[] stringCmds = new String[]{"kill", "-9", id};

        //exec cmd
        OutputStream out = null;
        try {
            Process pro = Runtime.getRuntime().exec(stringCmds);
            pro.waitFor();
            out = pro.getOutputStream();
            String result = "";
            OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
            writer.write(result);
            System.out.print("\n&&&stopProgress INFO:"+result);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startStreamJob(int streamEngineType, int jsonFilePath, String cmdFilePath){
        switch(streamEngineType){
            case STREAM_FLINK_ENGINE :
            {
                //generate cmd
                String[] stringCmds = new String[]{};
                switch(jsonFilePath) {
                    case SAVE_TO_HDFS:
                        stringCmds = new String[]{"sh", jsonFileDir + "/flink-startup.sh", cmdFilePath};
                        break;
                    case SAVE_TO_FILE:
                        break;
                    default:
                        break;
                }
                System.out.print("\n&&&startupProgress cmd:" + stringCmds.toString());
                //exec cmd
                OutputStream out = null;
                try {
                    Process pro = Runtime.getRuntime().exec(stringCmds);
                    pro.waitFor();
                    out = pro.getOutputStream();
                    String result = "";
                    OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
                    writer.write(result);
                    System.out.print("\n&&&startupProgress INFO:"+result);
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            default:
                break;
        }

    }

    private String getStreamPid(String jsonFilePath) {
        OutputStream out = null;
        String tmpFilePath = jsonFileDir + "/tmpStreamPid.txt" + System.currentTimeMillis();
        String result = "";
        String line = "";

        boolean getStreamIdSuccess = true;
        do {
            try {
                getStreamIdSuccess = true;
                Process pro = Runtime.getRuntime().exec(new String[]{"sudo", "sh", jsonFileDir + "/getStreamPid.sh", jsonFilePath, tmpFilePath});
                pro.waitFor();
                out = pro.getOutputStream();
                OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
                writer.write(result);
                System.out.print("\n&&&getStreamPid INFO:"+result);
                out.close();

                File pidFile = new File(tmpFilePath);
                InputStreamReader reader = new InputStreamReader(new FileInputStream(pidFile));
                BufferedReader buffer = new BufferedReader(reader);

                line = buffer.readLine();
                System.out.print("\n&&&getStreamPid streamPid:" + line + "\n");
            } catch (Exception e) { getStreamIdSuccess = false; }
        }while(!getStreamIdSuccess);

        return line;
    }

    private String getCmdParam(String keyName) {
        return mapCmdParams.get(keyName);
    }

    private int getCmdType(String cmd) {
        //init cmd type
        int cmdType = CREATE_STREAMJOB;
        switch(cmdType) {
            case CREATE_STREAMJOB: {
                String regExCreateStream = "^([ ]*CREATE[ ]+STREAMJOB[ ]+)([a-zA-Z0-9]+)([ ]+TBLPROPERTIES[ ]*\\(\\\"jobdef\\\"=\\\")([a-zA-Z0-9]+)(\\\"\\)[ ]*)$";
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
                String regExShowStream = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
                Pattern pattern = Pattern.compile(regExShowStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    break;
                } else {
                    cmdType = START_STREAMJOB;
                }
            }
            case START_STREAMJOB: {
                String regExStartStream = "(^[ ]*start[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
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
                String regExStopStream = "(^[ ]*stop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
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
                String regExDropStream = "(^[ ]*drop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
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
                String regExDescStream = "^([ ]*describe[ ]+)([a-zA-Z0-9]+)([ ]*)$";
                Pattern pattern = Pattern.compile(regExDescStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } //else //do nothing
            }*/

            default:
                break;
        }

        return cmdType;
    }

    //@Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        // do nothing
    }

}
