package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class StreamQLParserTest {

    //stremjob management
    String createSql = "CREATE STREAMJOB db.streamTest TBLPROPERTIES (\"output\"=\"/streamingPro/flink.json\")";
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
    StreamQLParser createStreamSqlParser = new StreamQLParser("CREATE STREAM db.streamTest(a int) TBLPROPERTIES (\"testKey\"=\"testValue\\a.txt\")");
    StreamQLParser showStreamSqlParser = new StreamQLParser("SHOW STREAMS");
    StreamQLParser dropStreamSqlParser = new StreamQLParser("DROP STREAM db.streamTest");
    StreamQLParser insertSelectSqlWinParser = new StreamQLParser("INSERT INTO db.output SELECT db.col, db.col2 FROM db.input STREAMWINDOW win AS (LENGTH 1 SECOND SLIDE 1 SECOND)");
    StreamQLParser insertSelectSqlNormal1Parser = new StreamQLParser("INSERT INTO db.output SELECT db.col, db.col2 FROM db.input");
    StreamQLParser insertSelectSqlNormal2Parser = new StreamQLParser("INSERT INTO db.output SELECT db.col, db.col2 FROM db.input join db.input2 as b");
    //StreamQLParser insertSelectSqlParser = new StreamQLParser("INSERT INTO db.output SELECT * FROM db.input STREAMWINDOW win AS (LENGTH 1 SECOND SLIDE 1 SECOND)");
    //grant management
    
    @Test
    public void init() throws Exception {
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
        assertEquals(createSqlParser.getStreamJobName(), "db.streamTest");
        assertEquals(showSqlParser.getStreamJobName(), null);
        assertEquals(startSqlParser.getStreamJobName(), "db.streamTest");
        assertEquals(stopSqlParser.getStreamJobName(), "db.streamTest");
        assertEquals(dropSqlParser.getStreamJobName(), "db.streamTest");
        assertEquals(createStreamSqlParser.getStreamJobName(), null);
        assertEquals(showStreamSqlParser.getStreamJobName(), null);
        assertEquals(dropStreamSqlParser.getStreamJobName(), null);
    }

    @Test
    public void getStreamOutput() throws Exception {
        init();
        assertEquals(createSqlParser.getStreamOutput(), "/streamingPro/flink.json");
        assertEquals(showSqlParser.getStreamOutput(), null);
        assertEquals(startSqlParser.getStreamOutput(), null);
        assertEquals(stopSqlParser.getStreamOutput(), null);
        assertEquals(dropSqlParser.getStreamOutput(), null);
        assertEquals(createStreamSqlParser.getStreamOutput(), null);
        assertEquals(showStreamSqlParser.getStreamOutput(), null);
        assertEquals(dropStreamSqlParser.getStreamOutput(), null);
    }

    @Test
    public void getTransformSql() throws Exception {
        init();
        assertEquals(createSqlParser.getTransformSql(), createSql);
        assertEquals(showSqlParser.getTransformSql(), showSql);
        assertEquals(startSqlParser.getTransformSql(), startSql);
        assertEquals(stopSqlParser.getTransformSql(), stopSql);
        assertEquals(dropSqlParser.getTransformSql(), dropSql);
        assertEquals(createStreamSqlParser.getTransformSql(), "CREATE TABLE db.streamTest(a int) TBLPROPERTIES (\"testKey\"=\"testValue\\a.txt\")");
        assertEquals(showStreamSqlParser.getTransformSql(), "SHOW TABLES");
        assertEquals(dropStreamSqlParser.getTransformSql(), "DROP TABLE db.streamTest");
    }

    @Test
    public void getTableList() throws Exception {
        init();
        String sql = "mjw) bb) (tok_tabref (tok_tabname mjw cc))))";
        String sql2 = "(tok_from (tok_join (tok_tabref (tok_tabname mjw) bb) (tok_tabref (tok_tabname mjw cc))))";
        StringBuilder builder = new StringBuilder();
        createSqlParser.getTableList(sql, builder);
        System.out.print(builder.toString());
    }
}