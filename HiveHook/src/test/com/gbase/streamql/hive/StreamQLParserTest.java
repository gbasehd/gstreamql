package com.gbase.streamql.hive;

import org.junit.Test;

import static org.junit.Assert.*;

public class StreamQLParserTest {

    //stremjob management
    String createSql = "CREATE STREAMJOB db.streamTest TBLPROPERTIES (\"jobdef\"=\"/streamingPro/flink.json\")";
    String showSql = "SHOW STREAMJOBS";
    String startSql = "START STREAMJOB db.streamTest";
    String stopSql = "stop streamjob db.streamTest";
    String dropSql = "drop streamjob db.streamTest";
    StreamQLParser createSqlParser = new StreamQLParser(createSql);
    StreamQLParser showSqlParser = new StreamQLParser(showSql);
    StreamQLParser startSqlParser = new StreamQLParser(startSql);
    StreamQLParser stopSqlParser = new StreamQLParser(stopSql);
    StreamQLParser dropSqlParser = new StreamQLParser(dropSql);
    //stream management
    StreamQLParser createStreamSqlParser = new StreamQLParser("CREATE STREAM db.streamTest(a int) TBLPROPERTIES (\"testKey\"=\"\testValue\\a.txt\")");
    StreamQLParser showStreamSqlParser = new StreamQLParser("SHOW STREAMS");
    StreamQLParser dropStreamSqlParser = new StreamQLParser("DROP STREAM db.streamTest");
    StreamQLParser insertSelectSqlParser = new StreamQLParser("INSERT INTO db.output SELECT db.col, db.col2 FROM db.input STREAMWINDOW win AS (LENGTH 1 SECOND SLIDE 1 SECOND)");
    //StreamQLParser insertSelectSqlParser = new StreamQLParser("INSERT INTO db.output SELECT * FROM db.input STREAMWINDOW win AS (LENGTH 1 SECOND SLIDE 1 SECOND)");
    //grant management
    
    private void init() {
        createSqlParser.parse();
        showSqlParser.parse();
        startSqlParser.parse();
        stopSqlParser.parse();
        dropSqlParser.parse();
        createStreamSqlParser.parse();
        showStreamSqlParser.parse();
        dropStreamSqlParser.parse();
    }

    @Test
    public void getCmdType() throws Exception {
        init();
        assertEquals(createSqlParser.getCmdType(), CMD.CREATE_STREAMJOB);
        assertEquals(showSqlParser.getCmdType(), CMD.SHOW_STREAMJOBS);
        assertEquals(startSqlParser.getCmdType(), CMD.START_STREAMJOB);
        assertEquals(stopSqlParser.getCmdType(), CMD.STOP_STREAMJOB);
        assertEquals(dropSqlParser.getCmdType(), CMD.DROP_STREAMJOB);
        assertEquals(createStreamSqlParser.getCmdType(), CMD.CREATE_STREAM);
        assertEquals(showStreamSqlParser.getCmdType(), CMD.SHOW_STREAMS);
        assertEquals(dropStreamSqlParser.getCmdType(), CMD.DROP_STREAM);
    }

    @Test
    public void getStreamJobName() throws Exception {
        init();
        assertEquals(createSqlParser.getStreamJobName(), "streamTest");
        assertEquals(showSqlParser.getStreamJobName(), null);
        assertEquals(startSqlParser.getStreamJobName(), "streamTest");
        assertEquals(stopSqlParser.getStreamJobName(), "streamTest");
        assertEquals(dropSqlParser.getStreamJobName(), "streamTest");
        assertEquals(createStreamSqlParser.getStreamJobName(), null);
        assertEquals(showStreamSqlParser.getStreamJobName(), null);
        assertEquals(dropStreamSqlParser.getStreamJobName(), null);
    }

    @Test
    public void getStreamJobDef() throws Exception {
        init();
        assertEquals(createSqlParser.getStreamJobDef(), "/streamingPro/flink.json");
        assertEquals(showSqlParser.getStreamJobDef(), null);
        assertEquals(startSqlParser.getStreamJobDef(), null);
        assertEquals(stopSqlParser.getStreamJobDef(), null);
        assertEquals(dropSqlParser.getStreamJobDef(), null);
        assertEquals(createStreamSqlParser.getStreamJobDef(), null);
        assertEquals(showStreamSqlParser.getStreamJobDef(), null);
        assertEquals(dropStreamSqlParser.getStreamJobDef(), null);
    }

    @Test
    public void getTransformSql() throws Exception {
        init();
        assertEquals(createSqlParser.getTransformSql(), createSql);
        assertEquals(showSqlParser.getTransformSql(), showSql);
        assertEquals(startSqlParser.getTransformSql(), startSql);
        assertEquals(stopSqlParser.getTransformSql(), stopSql);
        assertEquals(dropSqlParser.getTransformSql(), dropSql);
        assertEquals(createStreamSqlParser.getTransformSql(), "CREATE TABLE streamTest(a int) TBLPROPERTIES (\"testKey\"=\"testValue\")");
        assertEquals(showStreamSqlParser.getTransformSql(), "SHOW TABLES");
        assertEquals(dropStreamSqlParser.getTransformSql(), "DROP TABLE streamTest");
    }
}