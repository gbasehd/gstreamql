package com.gbase.streamql.hive;

import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamJobTest {
    @Test
    public void stopStreamJob() throws Exception {

    }

    @Test
    public void startStreamJob() throws Exception {
        StreamJobMetaData metaData = EasyMock.createMock(StreamJobMetaData.class);
        EasyMock.expect(metaData.getName()).andReturn("streamtest");
        EasyMock.expect(metaData.getDefine()).andReturn("/streamingPro/flink.json");
        EasyMock.replay(metaData);

        StreamQLConf conf = EasyMock.createMock(StreamQLConf.class);
        EasyMock.expect(conf.getJsonFileDir()).andReturn("/home/mjw");
        StreamJob job = new StreamJob(conf);
        job.startStreamJob(ENGINE.FLINK, FS.HDFS, metaData);
        EasyMock.verify(metaData);
    }

    @Test
    public void getStreamJobId() throws Exception {
    }

    @Test
    public void getStreamPid() throws Exception {
    }

}