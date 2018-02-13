package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.*;

public class StreamQLDriverRunHookTest {


    @Test
    public void preDriverRun() throws Exception {
        HiveDriverRunHookContext hookContext = EasyMock.createMock(HiveDriverRunHookContext.class);//创建Mock对象
        EasyMock.expect(hookContext.getCommand()).andReturn("CREATE STREAMJOB streamTest TBLPROPERTIES (\"jobdef\"=\"/streamingPro/flink.json\")");
        StreamJobMetaData metaData = EasyMock.createMock(StreamJobMetaData.class);
        EasyMock.expect(Utility.getStreamJobMetaData("streamTest")).andReturn(metaData);



    }




}