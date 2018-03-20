package com.gbase.streamql.hive;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamQLBuilderTest {
    @Test
    public void getSql() throws Exception {
        //mock parser
        StreamQLParser parser = EasyMock.createMock(StreamQLParser.class);
        //mock job
        StreamJob job = EasyMock.createMock(StreamJob.class);
        //mock conf
        Conf conf = EasyMock.createMock(Conf.class);
        conf.SYS_DB = "systest";
        EasyMock.replay(conf);

        /*****CREATE_STREAMJOB*************/
        EasyMock.expect(parser.getStreamJobName()).andReturn("streamtest");
        EasyMock.expect(parser.getStreamOutput()).andReturn("/streamingPro/flink.json");
        EasyMock.expect(parser.getCmdType()).andReturn(CMD.CREATE_STREAMJOB);
        EasyMock.replay(parser);
        //new builder
        StreamQLBuilder createBuilder = new StreamQLBuilder(parser, job);
        //verify
        assertEquals(createBuilder.getSql(), "Insert into systest.streamjobmgr(name, pid, jobid, status, define) values ('streamtest',NULL,NULL,'STOPPED','/streamingPro/flink.json')");
        EasyMock.verify(parser);
        EasyMock.verify(conf);

        /*************SHOW_STREAMJOBS*************/
        EasyMock.reset(parser);
        EasyMock.expect(parser.getStreamJobName()).andReturn("streamtest");
        EasyMock.expect(parser.getStreamOutput()).andReturn("/streamingPro/flink.json");
        EasyMock.expect(parser.getCmdType()).andReturn(CMD.SHOW_STREAMJOBS);
        EasyMock.replay(parser);
        //new builder
        StreamQLBuilder showBuilder = new StreamQLBuilder(parser, job);
        //verify
        assertEquals(showBuilder.getSql(), "Select name, jobid, status, define from systest.streamjobmgr");

        /*************START_STREAMJOB*************/
        EasyMock.reset(parser);
        EasyMock.expect(parser.getStreamJobName()).andReturn("streamtest");
        EasyMock.expect(parser.getStreamOutput()).andReturn("/streamingPro/flink.json");
        EasyMock.expect(parser.getCmdType()).andReturn(CMD.START_STREAMJOB);
        EasyMock.replay(parser);

        EasyMock.reset(job);
        EasyMock.expect(job.getPid()).andReturn("00000");
        EasyMock.expect(job.getJobId()).andReturn("11111");
        EasyMock.replay(job);
        //new builder
        StreamQLBuilder startBuilder = new StreamQLBuilder(parser, job);
        //verify
        assertEquals(startBuilder.getSql(), "Update systest.streamjobmgr set status = 'RUNNING' , pid = \"00000\", jobid = \"11111\" where name = \"streamtest\"");

        /*************STOP_STREAMJOB*************/
        EasyMock.reset(parser);
        EasyMock.expect(parser.getStreamJobName()).andReturn("streamtest");
        EasyMock.expect(parser.getStreamOutput()).andReturn("/streamingPro/flink.json");
        EasyMock.expect(parser.getCmdType()).andReturn(CMD.STOP_STREAMJOB);
        EasyMock.replay(parser);
        //new builder
        StreamQLBuilder stopBuilder = new StreamQLBuilder(parser, job);
        //verify
        assertEquals(stopBuilder.getSql(), "Update systest.streamjobmgr set status = 'STOPPED' , pid = \"NULL\", jobid = \"NULL\" where  name = \"streamtest\"");


        /*************DROP_STREAMJOB*************/
        EasyMock.reset(parser);
        EasyMock.expect(parser.getStreamJobName()).andReturn("streamtest");
        EasyMock.expect(parser.getStreamOutput()).andReturn("/streamingPro/flink.json");
        EasyMock.expect(parser.getCmdType()).andReturn(CMD.DROP_STREAMJOB);
        EasyMock.replay(parser);
        //new builder
        StreamQLBuilder dropBuilder = new StreamQLBuilder(parser, job);
        //verify
        assertEquals(dropBuilder.getSql(), "Delete from systest.streamjobmgr where  name = \"streamtest\"");

    }

}