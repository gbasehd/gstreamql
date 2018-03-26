package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.easymock.EasyMock;
import org.junit.Test;
import org.stringtemplate.v4.ST;

import java.util.HashMap;

import static org.junit.Assert.*;

public class StreamQLParserTest {

    //stremjob management
    boolean detailed = true;
    String createSql = "CREATE STREAMJOB db.streamTest TBLPROPERTIES (\"input\"=\"i\", \"output\"=\"o\")";
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
    private void printDetailed(String info){
        if (detailed) {
            System.out.println(info);
        }
    }
    @Test
    public void getStreamJobName() throws Exception {
        init();
        printDetailed(createSqlParser.getStreamJobName());
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
        assertEquals(createSqlParser.getStreamOutput(), "o");
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
        Utility.getTableList(sql, builder);
        System.out.print(builder.toString());
    }

    @Test
    public void testParser() throws ParseException {
        ParseDriver driver = new ParseDriver();

        String wrongSql = "insert into aaa";
        //driver.parse(wrongSql);

        String insertSql = "insert into bb select count(f0) from aa group by HOP(userActionTime,interval '3' second,interval '6' second)";
        ASTNode insert = (ASTNode) driver.parse(insertSql).getChild(0);
        StringBuilder inputStreams = new StringBuilder();
        StringBuilder outputStreams = new StringBuilder();
        Utility.getTableList(insert.getChild(0).toStringTree(), inputStreams);
        Utility.getTableList(insert.getChild(1).toStringTree(), outputStreams);
        assertEquals(inputStreams.toString(), "aa,");
        assertEquals(outputStreams.toString(), "bb,");


        String insertSql2 = "insert into bb select count(f0) from test group by TUMBLE(userActionTime,interval '3' second)";
        ASTNode insert2 = (ASTNode) driver.parse(insertSql2).getChild(0);
        StringBuilder inputStreams2 = new StringBuilder();
        StringBuilder outputStreams2 = new StringBuilder();
        Utility.getTableList(insert.getChild(0).toStringTree(), inputStreams2);
        Utility.getTableList(insert.getChild(1).toStringTree(), outputStreams2);
        assertEquals(inputStreams2.toString(), "aa,");
        assertEquals(outputStreams2.toString(), "bb,");

        String insertSql3 = "insert into bb SELECT *,COUNT(f0) OVER (PARTITION BY f0 ORDER BY userActionTime ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) FROM test";
        ASTNode insert3 = (ASTNode) driver.parse(insertSql3).getChild(0);
        StringBuilder inputStreams3 = new StringBuilder();
        StringBuilder outputStreams3 = new StringBuilder();
        Utility.getTableList(insert.getChild(0).toStringTree(), inputStreams3);
        Utility.getTableList(insert.getChild(1).toStringTree(), outputStreams3);
        assertEquals(inputStreams3.toString(), "aa,");
        assertEquals(outputStreams3.toString(), "bb,");

        String selectSql = "select * from a";
        driver.parse(selectSql);

        String expr = "a+b";
        driver.parseExpression(expr);

        String expr2 = "abs(1)";
        driver.parseExpression(expr2);

        //driver.parse(expr);

    }
}