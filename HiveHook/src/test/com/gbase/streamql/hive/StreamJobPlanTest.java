package com.gbase.streamql.hive;

import org.junit.Test;
import static org.junit.Assert.*;

public class StreamJobPlanTest {
    @Test
    public void case1() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String content = plan.getContent();
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
        System.out.println(content);
    }

    @Test
    public void case2() throws Exception {
        String[] input = {"i1","i2"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        try{
            String content = plan.getContent();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("The input stream 'i1, i2' cannot match"));
        }
    }

    @Test
    public void case3() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        try{
            String content = plan.getContent();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("The output stream 's' does not exist"));
        }
    }
}