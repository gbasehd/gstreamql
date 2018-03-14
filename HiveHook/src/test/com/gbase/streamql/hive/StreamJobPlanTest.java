package com.gbase.streamql.hive;

import org.junit.Test;
import static org.junit.Assert.*;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;

public class StreamJobPlanTest {
    private StreamRelation relation;
    public StreamJobPlanTest() throws Exception{
        relation = EasyMock.createMock(StreamRelation.class);
        expect(relation.hasSql(matches("^o$|(^d[1-4]$)"))).andStubReturn(true);
        expect(relation.hasSql(not(matches("^o$|(^d[1-4]$)")))).andStubReturn(false);
        expect(relation.hasPrev(matches("^o$|(^d[1-4]$)"))).andStubReturn(true);
        expect(relation.hasPrev(not(matches("^o$|(^d[1-4]$)")))).andStubReturn(false);
        expect(relation.isOutput("o")).andStubReturn(true);
        expect(relation.isOutput(not(eq("o")))).andStubReturn(false);
        String[] prevs = {"d3","d4"};
        expect(relation.getPrev(eq("o"))).andStubReturn(prevs);
        String[] prevs1 = {"i1","i2"};
        expect(relation.getPrev(eq("d1"))).andStubReturn(prevs1);
        String[] prevs2 = {"i3"};
        expect(relation.getPrev(eq("d2"))).andStubReturn(prevs2);
        expect(relation.getPrev(eq("d3"))).andStubReturn(prevs2);
        String[] prevs3 = {"d1","d2"};
        expect(relation.getPrev(eq("d4"))).andStubReturn(prevs3);
        expect(relation.getSql(eq("o"))).andStubReturn("select … from d3 join d4 …");
        expect(relation.getSql(eq("d1"))).andStubReturn("select … from i1 join i2 …");
        expect(relation.getSql(eq("d2"))).andStubReturn("select … from i3 …");
        expect(relation.getSql(eq("d3"))).andStubReturn("select … from i3 …");
        expect(relation.getSql(eq("d4"))).andStubReturn("select … from d1 join d2 …");
        replay(relation);
    }

    @Test
    public void case10() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        content = plan.print();
        System.out.println(json);
        System.out.println(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
        verify(relation);
    }

    @Test
    public void case11() throws Exception {
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
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
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
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
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
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
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
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
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
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

    @Test
    public void case24() throws Exception {
        String[] input = {};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
        try{
            plan.generate();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("Input cannot be empty!"));
        }
    }

    @Test
    public void case25() throws Exception {
        String[] input = {"i1","i2"};
        String output = "";
        StreamJobPlan plan = new StreamJobPlan(relation,input,output);
        try{
            plan.generate();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            assert(e.getMessage().equals("Output cannot be empty!"));
        }
    }

}