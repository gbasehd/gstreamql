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
                sql = "Insert into " +
                       Conf.SYS_DB + ".streamjobmgr(name, pid, jobid, status, define) values ('" +
                       this.parser.getStreamJobName() + "',NULL,NULL,'STOPPED','" +
                       this.parser.getStreamJobDef() + "')";
                break;
            }
            case SHOW_STREAMJOBS: {
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
            case UNMATCHED:
                break;
            default:
                break;
        }
        return sql;
    }
}
