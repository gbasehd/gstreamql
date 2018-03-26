package com.gbase.streamql.hive;

public class StreamQLBuilder {

    private StreamQLParser parser;
    private StreamJob job;

    public StreamQLBuilder(StreamQLParser parser, StreamJob job){
       this.parser = parser;
       this.job = job;
    }

    public String getSql() throws Exception {
        String sql = "";
        switch (parser.getCmdType()) {
            case CREATE_STREAMJOB: {
                //get plan
                //replace all space
                String[] inputs = this.parser.getStreamInput().replaceAll(" ", "").split(",");
                StreamJobPlan plan = new StreamJobPlan(inputs, parser.getStreamOutput());
                plan.generate();
                String hdfsFilePath = Utility.uploadHdfsFile(plan.getJson());
                sql = "Insert into " +
                       Conf.SYS_DB + ".streamjobmgr(name, pid, jobid, status, define, filepath) values ('" +
                       this.parser.getStreamJobName() + "',NULL,NULL,'STOPPED','input:" + parser.getStreamInput()
                        + ";ouput:" + parser.getStreamOutput() + "','" + hdfsFilePath + "')";
                Utility.Logger("SQL:" + sql);
                break;
            }
            case SHOW_STREAMJOBS: {
                Utility.updateStreamJobStatus();
                sql = "Select name, jobid, status, define from " + Conf.SYS_DB + ".streamjobmgr";
                break;
            }
            case START_STREAMJOB: {
                sql = "Update " +
                        Conf.SYS_DB +".streamjobmgr set status = '" + STATUS.RUNNING.toString() +
                       "' , pid = \"" + this.job.getPid() +
                       "\", jobid = \"" + this.job.getJobId() +
                       "\" where name = \"" + this.parser.getStreamJobName() + "\"";
                break;
            }
            case STOP_STREAMJOB: {
                Utility.updateStreamJobStatus();
                sql = "Update " +
                        Conf.SYS_DB +".streamjobmgr set status = '" + STATUS.STOPPED.toString() +
                       "' , pid = \"NULL\", jobid = \"NULL\" where  name = \"" + this.parser.getStreamJobName() + "\"";
                break;
            }
            case DROP_STREAMJOB: {
                sql = "Delete from " +
                        Conf.SYS_DB +".streamjobmgr where  name = \"" + this.parser.getStreamJobName() + "\"";
                break;
            }
            case CREATE_STREAM:
            case SHOW_STREAMS:
            case DROP_STREAM:
            case INSERT_STREAM:
            case EXPLAIN_PLAN:
            case UNMATCHED:
            default:
                sql = parser.getTransformSql();
                break;
        }
        return sql;
    }
}
