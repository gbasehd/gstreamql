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
import org.apache.hadoop.hive.ql.session.SessionState;

public class StreamQLDriverRunHook implements HiveDriverRunHook {

    StreamQLConf conf = new StreamQLConf();
    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        Logger("change "+ hookContext.getCommand());
        String cmd = hookContext.getCommand();
        StreamQLParser parser = new StreamQLParser(cmd);
        parser.parse();
        StreamJob job = new StreamJob(conf);
        String myCmd = "";
        switch(CMD.valueOf(parser.getCmdType())) {
            case CREATE_STREAMJOB: {
                //check if streamjob name exists
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(parser.getStreamJobName());
                if(jobMetaData != null && jobMetaData.getName().equals(parser.getStreamJobName())) {
                    throw  new Exception("Create stream job error! Stream job name \"" + parser.getStreamJobName() + "\" exists!");
                }
                //insert data
                myCmd = "Insert into " + conf.getDbName() + ".streamjobmgr(name, pid, jobid, status, define) values ('" + parser.getStreamJobName() + "',NULL,NULL,'STOPPED','" + parser.getStreamJobDef() + "')";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case SHOW_STREAMJOBS: {
                //TODO
                //check status again

                myCmd = "Select name, jobid, status, define from " + conf.getDbName() + ".streamjobmgr";
                Utility.setCmd(cmd, myCmd);
                break;
            }
            case START_STREAMJOB: {
                //1.Select define,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(parser.getStreamJobName());
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Start stream job failed! create stream job \"" + parser.getStreamJobName() + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS.STOPPED.toString())) {
                    //3.exec stremingPro
                    job.startStreamJob(ENGINE.FLINK, FS.HDFS, jobMetaData);
                    //4.Update ghd.streamjobmgr set status = 'RUNNING' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + conf.getDbName() +".streamjobmgr set status = '" + STATUS.RUNNING.toString() +
                            "' , pid = \"" + job.getStreamPid(jobMetaData.getDefine()) +
                            "\", jobid = \"" + job.getStreamJobId(parser.getStreamJobName()) +
                            "\" where  name = \"" + parser.getStreamJobName() + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    throw new Exception("Execute error! target stream job is running!");
                }
                break;
            }
            case STOP_STREAMJOB: {
                //1.Select id,status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(parser.getStreamJobName());
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Stop stream job failed! create stream job \"" + parser.getStreamJobName() + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS.RUNNING.toString())) {
                    //3.Kill streamjob_id
                    job.stopStreamJob(jobMetaData);
                    //4.Update ghd.streamjobmgr set status = 'STOPPED' , id = <jobid> where  name = <streamjob_name>
                    myCmd = "Update " + conf.getDbName() +".streamjobmgr set status = '" + STATUS.STOPPED.toString() +
                            "' , pid = \"NULL\", jobid = \"NULL\" where  name = \"" + parser.getStreamJobName() + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    //do nothing
                }
                break;
            }
            case DROP_STREAMJOB: {
                //1.Select status from ghd.streamjobmgr where name = <streamjob_name>
                StreamJobMetaData jobMetaData = Utility.getStreamJobMetaData(parser.getStreamJobName());
                //2.check status
                if(jobMetaData == null)
                    throw new Exception("Drop stream job failed! create stream job \"" + parser.getStreamJobName() + "\" first!");
                if(jobMetaData.getStatus().equals(STATUS.STOPPED.toString())) {
                    //3.Delete from ghd.streamjobmgr where name = <streamjob_name>
                    myCmd = "Delete from " + conf.getDbName() +".streamjobmgr where  name = \"" + parser.getStreamJobName() + "\"";
                    Utility.setCmd(cmd, myCmd);
                } else {
                    throw new Exception("Execute error! Unable to delete the running job!");
                }
                break;
            }
            /*case DESCRIBE_STREAMJOB: {
                myCmd = "desc table" + parser.getStreamJobName();
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

    //@Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        // do nothing
    }

    private void Logger(String output) {
        if(conf.isDebug())
            System.out.print(output);
    }

}
