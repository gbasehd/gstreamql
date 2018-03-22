package com.gbase.streamql.hive;

import org.junit.Test;
import static org.junit.Assert.*;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import java.lang.reflect.Field;

public class StreamJobPlanTest {
    private StreamRelation relation;
    private boolean detailed = false;
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
        expect(relation.getSql(eq("o"))).andStubReturn("select * from d3 join d4 ");
        expect(relation.getSql(eq("d1"))).andStubReturn("select * from i1 join i2 ");
        expect(relation.getSql(eq("d2"))).andStubReturn("select * from i3 ");
        expect(relation.getSql(eq("d3"))).andStubReturn("select * from i3 ");
        expect(relation.getSql(eq("d4"))).andStubReturn("select * from d1 join d2 ");
        replay(relation);
    }

    private void printDetailed(String info){
        if (detailed) {
            System.out.println(info);
        }
    }

    //set StreamJobPlan.relation to mock obj
    private void setRelationMock(StreamJobPlan plan, StreamRelation relation) throws Exception{
        Field field= plan.getClass().getDeclaredField("relation");
        field.setAccessible(true);
        field.set(plan, relation);
    }

    @Test
    public void case10() throws Exception {
        printDetailed(">>>>>>> case10:");
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        content = plan.print();
        printDetailed(json);
        printDetailed(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
        verify(relation);
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case11() throws Exception {
        printDetailed(">>>>>>> case11:");
        String[] input = {"i1","i2","i3"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        json = plan.getJson();
        content = plan.print();
        content = plan.print();
        printDetailed(json);
        printDetailed(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case12() throws Exception {
        printDetailed(">>>>>>> case12:");
        String inputs = "i1,i2,i3";
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan();
        plan.set(inputs,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        plan.generate();
        json = plan.getJson();
        json = plan.getJson();
        content = plan.print();
        content = plan.print();
        printDetailed(json);
        printDetailed(content);
        String expect = "[0]o->[0]d3->[0]i3(end) [1]d4->[1]d1->[1]i1(end) [2]i2(end) [3]d2->[3]i3(end) ";
        assert(content.equals(expect));
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case20() throws Exception {
        printDetailed(">>>>>>> case20:");
        String[] input = {"i1","i2"};
        String output = "o";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        try{
            plan.generate();
            json = plan.getJson();
            content = plan.print();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("The input stream 'i1, i2' cannot match"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case21() throws Exception {
        printDetailed(">>>>>>> case21:");
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        try{
            plan.generate();
            json = plan.getJson();
            content = plan.print();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("The output stream 's' does not exist"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case22() throws Exception {
        printDetailed(">>>>>>> case22:");
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        try{
            json = plan.getJson();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("Must first execute function StreamJobPlan.generate() to get json string"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case23() throws Exception {
        printDetailed(">>>>>>> case23:");
        String[] input = {"i1","i2","i3"};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        String json = "";
        String content = "";
        try{
            content = plan.print();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("Must first execute function StreamJobPlan.generate() to get plan content"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case24() throws Exception {
        printDetailed(">>>>>>> case24:");
        String[] input = {};
        String output = "s";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        try{
            plan.generate();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("Input cannot be empty!"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case25() throws Exception {
        printDetailed(">>>>>>> case25:");
        String[] input = {"i1","i2","i3"};
        String output = "";
        StreamJobPlan plan = new StreamJobPlan(input,output);
        setRelationMock(plan,this.relation);
        try{
            plan.generate();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("Output cannot be empty!"));
        }
        printDetailed("<<<<<<<< end");
    }

    @Test
    public void case26() throws Exception {
        printDetailed(">>>>>>> case26:");
        String[] input = {"i1","i2","i3"};
        String output = "";
        StreamJobPlan plan = new StreamJobPlan();
        setRelationMock(plan,this.relation);
        try{
            plan.generate();
        }
        catch (Exception e){
            printDetailed(e.getMessage());
            assert(e.getMessage().equals("Output cannot be empty!"));
        }
        printDetailed("<<<<<<<< end");
    }
}