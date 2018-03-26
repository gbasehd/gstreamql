package com.gbase.streamql.hive;

public class StreamJob {

    private String jobName;
    private StreamJobMetaData jobMetaData = null;
    private Utility util = new Utility();

    public StreamJob(String jobName) throws Exception {
        if(jobName == null || jobName.equals(""))
            throw new Exception("Generate instance failed, jobName is null!");
        this.jobName = jobName;
        this.jobMetaData = getMetaData();
    }

    public boolean isExists() {
        if(this.jobMetaData != null)
            return true;
        else
            return false;
    }

    public boolean isStopped(){
        if(this.jobMetaData.getStatus().equals(STATUS.STOPPED.toString()))
            return true;
        else
            return false;
    }

    public boolean isRunning() {
        if(this.jobMetaData.getStatus().equals(STATUS.RUNNING.toString()))
            return true;
        else
            return false;
    }

    public void stop() throws Exception {
        util.cancelFlinkJob(this.jobMetaData.getJobid());
        util.killPro(this.jobMetaData.getPid());
    }

    public void start() throws Exception {
        switch(Conf.JOB_ENG){
            case FLINK :
            {
                switch(Conf.JOB_TARGET) {
                    case HDFS:
                        util.startFlinkJob(this.jobMetaData.getName(),this.jobMetaData.getFilePath());
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

    public String getJobId() throws Exception {
        return util.getJobIdFromServer(this.jobName);
    }

    // tmp code
    public String getPid() throws InterruptedException {
         return util.getPid(this.jobMetaData.getFilePath());
    }

    private StreamJobMetaData getMetaData() throws Exception{
        if(this.jobName == null || this.jobName.equals(""))
            throw new Exception("GetMetaData error, jobName undefined!");
        return util.getMetaFromHive(this.jobName);
    }

}
