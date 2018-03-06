package com.gbase.streamql.hive;

import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamJobTest {
    @Test(expected = Exception.class)
    public void StreamJob() throws Exception {
        StreamJob jobNameIsNull = new StreamJob(null);
        StreamJob jobNameIsEmpty = new StreamJob("");
    }
}