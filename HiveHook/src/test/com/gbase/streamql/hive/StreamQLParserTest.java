package com.gbase.streamql.hive;

import org.junit.Test;

import static org.junit.Assert.*;

public class StreamQLParserTest {

    StreamQLParser createSql = new StreamQLParser("CREATE STREAMJOB streamTest TBLPROPERTIES (\"jobdef\"=\"/streamingPro/flink.json\")");
    StreamQLParser showSql = new StreamQLParser("SHOW STREAMJOBS");
    StreamQLParser startSql = new StreamQLParser("START STREAMJOB streamTest");
    StreamQLParser stopSql = new StreamQLParser("stop streamjob streamTest");
    StreamQLParser dropSql = new StreamQLParser("drop streamjob streamTest");

    @Test
    public void getCmdType() throws Exception {
        createSql.parse();
        showSql.parse();
        startSql.parse();
        stopSql.parse();
        dropSql.parse();
        assertEquals(createSql.getCmdType(), CMD.CREATE_STREAMJOB);
        assertEquals(showSql.getCmdType(), CMD.SHOW_STREAMJOBS);
        assertEquals(startSql.getCmdType(), CMD.START_STREAMJOB);
        assertEquals(stopSql.getCmdType(), CMD.STOP_STREAMJOB);
        assertEquals(dropSql.getCmdType(), CMD.DROP_STREAMJOB);
    }

    @Test
    public void getStreamJobName() throws Exception {
        createSql.parse();
        showSql.parse();
        startSql.parse();
        stopSql.parse();
        dropSql.parse();
        assertEquals(createSql.getStreamJobName(), "streamTest");
        assertEquals(showSql.getStreamJobName(), null);
        assertEquals(startSql.getStreamJobName(), "streamTest");
        assertEquals(stopSql.getStreamJobName(), "streamTest");
        assertEquals(dropSql.getStreamJobName(), "streamTest");
    }

    @Test
    public void getStreamJobDef() throws Exception {
        createSql.parse();
        showSql.parse();
        startSql.parse();
        stopSql.parse();
        dropSql.parse();
        assertEquals(createSql.getStreamJobDef(), "/streamingPro/flink.json");
        assertEquals(showSql.getStreamJobDef(), null);
        assertEquals(startSql.getStreamJobDef(), null);
        assertEquals(stopSql.getStreamJobDef(), null);
        assertEquals(dropSql.getStreamJobDef(), null);
    }

}