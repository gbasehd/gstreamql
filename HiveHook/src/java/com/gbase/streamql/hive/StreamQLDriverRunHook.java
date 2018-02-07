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

import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import java.lang.reflect.Field;
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

    //def cmd params
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_JOB_DEF = "streamJobDef";

    //store cmd params
    private Map<String, String> mapCmdParams = new HashMap<String, String>();

    //tmp param
    private String dbName = "mjw";

    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        SessionState.getConsole().getOutStream().println("change "+ hookContext.getCommand());
        String cmd = hookContext.getCommand();



        String myCmd = "";
        switch(getCmdType(cmd)) {
            case CREATE_STREAMJOB: {
                myCmd = "Insert into " + dbName + ".streamjobmgr values ('" + getCmdParam(STREAM_JOB_NAME) + "',NULL,'STOPPED','" + getCmdParam(STREAM_JOB_DEF) + "')";
                changeCmd(cmd, myCmd);
                break;
            }
            case SHOW_STREAMJOBS: {
                myCmd = "Select * from " + dbName + ".streamjobmgr";
                changeCmd(cmd, myCmd);
                break;
            }
            case START_STREAMJOB:
                break;
            case STOP_STREAMJOB:
                break;
            case DROP_STREAMJOB:
                break;
            case DESCRIBE_STREAMJOB:
                break;
            default:
                break;
        }



        SessionState.getConsole().getOutStream().println("to " + hookContext.getCommand());
    }

    private void changeCmd(String orgCmd, String myCmd) throws Exception{
        /////////
        Field valueFieldOfString = String.class.getDeclaredField("value");
        valueFieldOfString.setAccessible(true);
        valueFieldOfString.set(orgCmd, myCmd.toCharArray());
    }

    private String getCmdParam(String keyName) {
        return mapCmdParams.get(keyName);
    }

    private void execCmd(String myCmd) {
        //TODO
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
            case SHOW_STREAMJOBS:
                String regExShowStream = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
                Pattern pattern = Pattern.compile(regExShowStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if(matcher.matches()) {
                    break;
                } else {
                    cmdType = START_STREAMJOB;
                }
            case START_STREAMJOB:
                break;
            case STOP_STREAMJOB:
                break;
            case DROP_STREAMJOB:
                break;
            case DESCRIBE_STREAMJOB:
                break;
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
