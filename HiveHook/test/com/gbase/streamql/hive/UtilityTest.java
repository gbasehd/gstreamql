package com.gbase.streamql.hive;

import org.easymock.EasyMock;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class UtilityTest {
    @Test
    public void getStreamJobMetaData() throws Exception {
        HiveService mockHiveService = EasyMock.createMock(HiveService.class);

        Connection conn = EasyMock.createMock(Connection.class);
        Statement stat = EasyMock.createMock(Statement.class);
        EasyMock.expect(mockHiveService.getConn()).andReturn(conn);//录制Mock对象预期行为
        EasyMock.expect(mockHiveService.getStmt(conn)).andReturn(stat);

        ResultSet rs = EasyMock.createMock(ResultSet.class);
        EasyMock.expect(rs.getString(1)).andReturn("name");
        EasyMock.expect(rs.getString(2)).andReturn("pid");
        EasyMock.expect(rs.getString(3)).andReturn("jobid");
        EasyMock.expect(rs.getString(4)).andReturn("status");
        EasyMock.expect(rs.getString(5)).andReturn("define");

        EasyMock.expect(stat.executeQuery(
                "select name, pid, jobid, status, define from mjw.streamjobmgr where name = \"streamTest\""))
                .andReturn(rs);
        EasyMock.replay(mockHiveService);//重放Mock对象，测试时以录制的对象预期行为代替真实对象的行为

        Utility.getStreamJobMetaData("streamTest");
        EasyMock.verify(mockHiveService);
    }

}