package com.gbase.streamql.hive;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.easymock.EasyMock;
import org.junit.Test;
import java.lang.reflect.Array;
import static org.easymock.EasyMock.*;
import java.lang.reflect.Field;
import static org.junit.Assert.*;
import java.util.*;

public class StreamQLBuilderTest {
    private StreamJobPlan mockPlan;
    private Utility mockUtil;
    private StreamJob mockJob ;
    private Conf mockConf;
    private StreamQLParser mockCreateStreamJobParser;
    private StreamQLParser mockShowStreamJobsParser;
    private StreamQLParser mockStartStreamJobParser;
    private StreamQLParser mockStopStreamJobParser;
    private StreamQLParser mockDropStreamJobParser;
    private boolean detailed = false;

    public StreamQLBuilderTest() throws Exception{
        mockPlan = EasyMock.createMock(StreamJobPlan.class);
        String jsonStr = "123";
        expect(mockPlan.getJson()).andStubReturn(jsonStr);
        String printStr = "abc";
        expect(mockPlan.print()).andStubReturn(printStr);
        mockPlan.set( anyString(), anyString() );
        expectLastCall().anyTimes();
        mockPlan.generate();
        expectLastCall().anyTimes();
        replay(mockPlan);

        mockUtil = EasyMock.createMock(Utility.class);
        expect(mockUtil.getJobIdFromServer(anyString())).andStubReturn("a");
        StreamJobMetaData meta = new StreamJobMetaData();
        expect(mockUtil.getMetaFromHive(anyString())).andStubReturn(meta);
        expect(mockUtil.getPid(anyString())).andStubReturn("1234456");
        expect(mockUtil.uploadHdfsFile(anyString())).andStubReturn("/xx/xxxx");
        mockUtil.setCmd(anyString(),anyString());
        expectLastCall().anyTimes();
        mockUtil.cancelFlinkJob(anyString());
        expectLastCall().anyTimes();
        mockUtil.edgePersist(isA(Map.class));
        expectLastCall().anyTimes();
        mockUtil.killPro(anyString());
        expectLastCall().anyTimes();
        mockUtil.Logger(anyString());
        expectLastCall().anyTimes();
        mockUtil.startFlinkJob(anyString(),anyString());
        expectLastCall().anyTimes();
        mockUtil.updateStreamJobStatus();
        expectLastCall().anyTimes();
        replay(mockUtil);

        mockJob = EasyMock.createMock(StreamJob.class);
        expect(mockJob.getPid()).andStubReturn("00000");
        expect(mockJob.getJobId()).andStubReturn("11111");
        replay(mockJob);

        mockConf = EasyMock.createMock(Conf.class);
        mockConf.SYS_DB = "systest";
        replay(mockConf);

        mockCreateStreamJobParser = EasyMock.createMock(StreamQLParser.class);
        expect(mockCreateStreamJobParser.getStreamJobName()).andStubReturn("streamtest");
        expect(mockCreateStreamJobParser.getStreamOutput()).andStubReturn("o");
        expect(mockCreateStreamJobParser.getStreamInput()).andStubReturn("i1,i2");
        expect(mockCreateStreamJobParser.getCmdType()).andStubReturn(CMD.CREATE_STREAMJOB);
        replay(mockCreateStreamJobParser);

        mockShowStreamJobsParser = EasyMock.createMock(StreamQLParser.class);
        expect(mockShowStreamJobsParser.getStreamJobName()).andStubReturn("streamtest");
        expect(mockShowStreamJobsParser.getStreamOutput()).andStubReturn("/streamingPro/flink.json");
        expect(mockShowStreamJobsParser.getCmdType()).andStubReturn(CMD.SHOW_STREAMJOBS);
        replay(mockShowStreamJobsParser);

        mockStartStreamJobParser = EasyMock.createMock(StreamQLParser.class);
        expect(mockStartStreamJobParser.getStreamJobName()).andStubReturn("streamtest");
        expect(mockStartStreamJobParser.getStreamOutput()).andStubReturn("/streamingPro/flink.json");
        expect(mockStartStreamJobParser.getCmdType()).andStubReturn(CMD.START_STREAMJOB);
        replay(mockStartStreamJobParser);

        mockStopStreamJobParser = EasyMock.createMock(StreamQLParser.class);
        expect(mockStopStreamJobParser.getStreamJobName()).andStubReturn("streamtest");
        expect(mockStopStreamJobParser.getStreamOutput()).andStubReturn("/streamingPro/flink.json");
        expect(mockStopStreamJobParser.getCmdType()).andStubReturn(CMD.STOP_STREAMJOB);
        replay(mockStopStreamJobParser);

        mockDropStreamJobParser = EasyMock.createMock(StreamQLParser.class);
        expect(mockDropStreamJobParser.getStreamJobName()).andStubReturn("streamtest");
        expect(mockDropStreamJobParser.getStreamOutput()).andStubReturn("/streamingPro/flink.json");
        expect(mockDropStreamJobParser.getCmdType()).andStubReturn(CMD.DROP_STREAMJOB);
        replay(mockDropStreamJobParser);
    }

    //set StreamQLBuilder.plan to mock obj
    private void setPlanMock(StreamQLBuilder builder, StreamJobPlan plan) throws Exception{
        Field field= builder.getClass().getDeclaredField("plan");
        field.setAccessible(true);
        field.set(builder, plan);
    }

    private void setUtilMock(StreamQLBuilder builder, Utility mockUtil) throws Exception{
        Field field= builder.getClass().getDeclaredField("util");
        field.setAccessible(true);
        field.set(builder, mockUtil);
    }
    private void printDetailed(String info){
        if (detailed) {
            System.out.println(info);
        }
    }

    @Test
    public void createStreamJob() throws Exception {
        printDetailed(">>>>>> Create Stream Job:");

        StreamQLBuilder createBuilder = new StreamQLBuilder(mockCreateStreamJobParser, mockJob);
        setPlanMock(createBuilder, this.mockPlan);
        setUtilMock(createBuilder, this.mockUtil);
        //verify
        printDetailed(createBuilder.getSql());
        assertEquals(createBuilder.getSql(),
                "Insert into systest.streamjobmgr(name, pid, jobid, status, define, filepath) " +
                        "values ('streamtest',NULL,NULL,'STOPPED','input:i1,i2;ouput:o','/xx/xxxx')");
        EasyMock.verify(mockCreateStreamJobParser);
        EasyMock.verify(mockJob);
        EasyMock.verify(mockConf);
        printDetailed("<<<<<< end");
    }

    @Test
    public void showStreamJobs() throws Exception {
        printDetailed(">>>>>> Show Stream Jobs:");
        StreamQLBuilder showBuilder = new StreamQLBuilder(mockShowStreamJobsParser, mockJob);
        setPlanMock(showBuilder, this.mockPlan);
        setUtilMock(showBuilder, this.mockUtil);
        //verify
        printDetailed(showBuilder.getSql());
        assertEquals(showBuilder.getSql(), "Select name, jobid, status, define from systest.streamjobmgr");
        EasyMock.verify(mockShowStreamJobsParser);
        EasyMock.verify(mockJob);
        printDetailed("<<<<<< end");
    }

    @Test
     public void startStreamJob() throws  Exception {
        printDetailed(">>>>>> Start Stream Jobs:");
        StreamQLBuilder startBuilder = new StreamQLBuilder(mockStartStreamJobParser, mockJob);
        setPlanMock(startBuilder, this.mockPlan);
        setUtilMock(startBuilder, this.mockUtil);
        //verify
        printDetailed(startBuilder.getSql());
        assertEquals(startBuilder.getSql(), "Update systest.streamjobmgr set status = 'RUNNING' , pid = \"00000\", jobid = \"11111\" where name = \"streamtest\"");
        EasyMock.verify(mockStartStreamJobParser);
        EasyMock.verify(mockJob);
        printDetailed("<<<<<< end");
    }

    @Test
    public void stopStreamJob() throws  Exception {
        printDetailed(">>>>>> Stop Stream Jobs:");
        StreamQLBuilder stopBuilder = new StreamQLBuilder(mockStopStreamJobParser, mockJob);
        setPlanMock(stopBuilder, this.mockPlan);
        setUtilMock(stopBuilder, this.mockUtil);
        //verify
        printDetailed(stopBuilder.getSql());
        assertEquals(stopBuilder.getSql(), "Update systest.streamjobmgr set status = 'STOPPED' , pid = \"NULL\", jobid = \"NULL\" where  name = \"streamtest\"");
        EasyMock.verify(mockStopStreamJobParser);
        EasyMock.verify(mockJob);
        printDetailed("<<<<<< end");
    }

    @Test
    public void dropStreamJob() throws  Exception {
        printDetailed(">>>>>> Drop Stream Jobs:");
        StreamQLBuilder dropBuilder = new StreamQLBuilder(mockDropStreamJobParser, mockJob);
        setPlanMock(dropBuilder,this.mockPlan);
        setUtilMock(dropBuilder,this.mockUtil);
        //verify
        printDetailed(dropBuilder.getSql());
        assertEquals(dropBuilder.getSql(), "Delete from systest.streamjobmgr where  name = \"streamtest\"");
        EasyMock.verify(mockDropStreamJobParser);
        EasyMock.verify(mockJob);
        printDetailed("<<<<<< end");
    }

}