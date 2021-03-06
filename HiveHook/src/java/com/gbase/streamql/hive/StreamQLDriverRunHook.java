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

package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;

public class StreamQLDriverRunHook implements HiveDriverRunHook{

    private Utility util = new Utility();
    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        Conf.Init();
        String cmd = hookContext.getCommand();
        StreamQLParser parser = new StreamQLParser(cmd);
        parser.parse();

        util.Logger("STEP INTO PRE DRIVER RUN");
        util.Logger("CMD TYPE:" + parser.getCmdType());
        StreamJob job = null;
        if (parser.getTransformSql() != null) {
            if(parser.getStreamJobName() != null && !parser.getStreamJobName().equals(""))
                job = new StreamJob(parser.getStreamJobName());
            StreamQLBuilder builder = new StreamQLBuilder(parser, job);
            Logger("change " + hookContext.getCommand() + " \n");
            realRun(cmd,parser,job,builder);
            Logger("to " + hookContext.getCommand() + "\n");
        }
    }

    //@Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        // do nothing
    }

    private void realRun(String cmd, StreamQLParser parser, StreamJob job, StreamQLBuilder builder) throws Exception {
        boolean isContinueHandle = false;
        String myCmd = "";
        switch(parser.getCmdType()) {
            case CREATE_STREAMJOB: {
                if( job.isExists()) {
                    throw new Exception("Create stream job error! Stream job name \"" +
                                         parser.getStreamJobName() + "\" exists!");
                }
                isContinueHandle = true;
                break;
            }
            case SHOW_STREAMJOBS: {
                //TODO
                //check status again
                isContinueHandle = true;
                break;
            }
            case START_STREAMJOB: {
                if(!job.isExists())
                    throw new Exception("Start stream job failed! Stream job \"" +
                                         parser.getStreamJobName() + "\" does not exist!");
                if(job.isStopped()) {
                    job.start();
                    isContinueHandle = true;
                } else {
                    throw new Exception("Execute error! target stream job is running!");
                }
                break;
            }
            case STOP_STREAMJOB: {
                if(!job.isExists())
                    throw new Exception("Stop stream job failed! Stream job \"" +
                                        parser.getStreamJobName() + "\" does not exist!");
                if(job.isRunning()) {
                    job.stop();
                    isContinueHandle = true;
                } else {
                    //do nothing
                }
                break;
            }
            case DROP_STREAMJOB: {
                if(!job.isExists())
                    throw new Exception("Drop stream job failed! Stream job \"" +
                                        parser.getStreamJobName() + "\" does not exist!");
                if(job.isStopped()) {
                    isContinueHandle = true;
                } else {
                    throw new Exception("Execute error! Unable to delete the running job!");
                }
                break;
            }
            case CREATE_STREAM:
            case SHOW_STREAMS:
            case DROP_STREAM:
            case INSERT_STREAM:
            case EXPLAIN_PLAN:
                isContinueHandle = true;
                break;
            case UNMATCHED:
                break;
            default:
                break;
        }
        if(isContinueHandle) {
            myCmd = builder.getSql();
            util.setCmd(cmd, myCmd);
        }
    }

    private void Logger(String output) {
        if(Conf.SYS_IS_DEBUG)
            System.out.print(output);
    }
}
