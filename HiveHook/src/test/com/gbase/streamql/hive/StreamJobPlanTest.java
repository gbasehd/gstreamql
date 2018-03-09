package com.gbase.streamql.hive;

import org.junit.Test;
import static org.junit.Assert.*;

public class StreamJobPlanTest {
    @Test
    public void case10() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        content = plan.print();
        System.out.println(json);
        System.out.println(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
    }

    @Test
    public void case11() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        json = plan.getJson();
        content = plan.print();
        content = plan.print();
        System.out.println(json);
        System.out.println(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
    }

    @Test
    public void case20() throws Exception {
        String[] input = {"i1","i2"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        try{
            plan.generate();
            json = plan.getJson();
            content = plan.print();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("The input stream 'i1, i2' cannot match"));
        }
    }

    @Test
    public void case21() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        try{
            plan.generate();
            json = plan.getJson();
            content = plan.print();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("The output stream 's' does not exist"));
        }
    }

    @Test
    public void case22() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        try{
            json = plan.getJson();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("Must first execute function StreamJobPlan.generate() to get json string"));
        }
    }

    @Test
    public void case23() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        String json = "";
        String content = "";
        try{
            content = plan.print();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("Must first execute function StreamJobPlan.generate() to get plan content"));
        }
    }

}