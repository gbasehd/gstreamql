package com.gbase.streamql.hive;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

import static com.gbase.streamql.hive.Utility.getTableList;

public class StreamQLParser {

    private String PATTERN_CREATE_STREAMJOB = "^([ ]*CREATE[ ]+STREAMJOB[ ]+)([a-zA-Z0-9\\.]+)([ ]+TBLPROPERTIES[ ]*\\(\\\"input\\\"=\\\")([a-zA-Z0-9,/\\.]+)(\\\"[, ]+\\\"output\\\"=\\\")([a-zA-Z0-9/\\.]+)(\\\"\\)[ ]*)$";
    private String PATTERN_SHOW_STREAMJOBS = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
    private String PATTERN_START_STREAMJOB = "(^[ ]*start[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_STOP_STREAMJOB = "(^[ ]*stop[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_DROP_STREAMJOB = "(^[ ]*drop[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_DESCRIBE_STREAMJOB = "^([ ]*describe[ ]+)([a-zA-Z/0-9]+)([ ]*)$";
    private String PATTERN_CREATE_STREAM = "(^[ ]*CREATE[ ]+STREAM[ ]+)([a-zA-Z\\.0-9]+)([ ]+TBLPROPERTIES.*)";
    private String PATTERN_SHOW_STREAMS = "^[ ]*SHOW[ ]+STREAMS[ ]*$";
    private String PATTERN_DROP_STREAM = "(^[ ]*drop[ ]+stream[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    //TODO
    /*private String PATTERN_INSERT_STREAM_WINDOW = "(^[ ]*insert[ ]+into[ ]+)([a-zA-Z0-9\\.]+)([ ]+select[ ]+.*from[ ]+)([a-zA-Z0-9\\.]+)" +
            "([ ]+STREAMWINDOW[ ]+[a-zA-Z0-9\\.]+[ ]+as[ ]*[(]" +
            "(SEPARATED[ ]+BY[ ]+[A-Za-z0-9]+[ ]+)?[ ]*" +
            "(LENGTH[ ]+[0-9]+[ ]+(SECOND|MINUTE)|INTERVAL[ ]+[0-9]+[ ]+(SECOND|MINUTE)|INFINITE)" +
            "([ ]+SLIDE[ ]+[0-9]+[ ]+(MINUTE|SECOND))?[ ]*[)][ ]*$)";*/
    /*private String PATTERN_INSERT_STREAM_WINDOW = "(^[ ]*insert[ ]+into[ ]+)([a-zA-Z0-9\\.]+)([ ]+SELECT[ ]+.*FROM[ ]+)([A-Za-z0-9\\.]+)" +
            "([ ]+GROUP[ ]+BY[ ]+" +
            "(TUMBLE[ ]*[(][A-Za-z0-9  ]+[,][ ]*INTERVAL[ ]+['][0-9]+['][ ]+SECOND[ ]*[)]" +
            "|HOP[ ]*[(][A-Za-z0-9  ]+[,][ ]*INTERVAL[ ]+['][0-9]+['][ ]+SECOND[ ]*[,][ ]*INTERVAL[ ]+['][0-9]+['][ ]+SECOND[ ]*[)])[ ]*$)";*/
    private String PATTERN_INSERT_STREAM_JOIN = "insert stream join";
    private String PATTERN_INSERT_sth = "(^[ ]*insert[ ]+into[ ]+[a-zA-Z0-9\\.]+[ ]+)(select.*)";
    private String PATTERN_EXPLAIN_PLAN = "^[ ]*explain[ ]+plan[ ]*$";
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_OUTPUT = "streamOutput";
    private String STREAM_INPUT = "streamInput";

    private CMD cmdType;
    private Map<String, String> mapCmdParams = new HashMap<String, String>();
    private String cmd;
    private Utility util = new Utility();

    public StreamQLParser (String cmd){
        this.cmd = cmd;
    }

    public void parse() throws Exception {
        //init cmd type
        CMD cmdType = CMD.CREATE_STREAMJOB;
        switch(cmdType) {
            case CREATE_STREAMJOB: {
                String regExCreateStream = PATTERN_CREATE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if(matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    mapCmdParams.put(STREAM_INPUT, matcher.group(4));
                    mapCmdParams.put(STREAM_OUTPUT, matcher.group(6));
                    this.cmdType = CMD.CREATE_STREAMJOB;
                    break;
                }
            }
            case SHOW_STREAMJOBS: {
                String regExShowStream = PATTERN_SHOW_STREAMJOBS;
                Pattern pattern = Pattern.compile(regExShowStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    this.cmdType = CMD.SHOW_STREAMJOBS;
                    break;
                }
            }
            case START_STREAMJOB: {
                String regExStartStream = PATTERN_START_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStartStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    this.cmdType = CMD.START_STREAMJOB;
                    break;
                }
            }
            case STOP_STREAMJOB: {
                String regExStopStream = PATTERN_STOP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStopStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    this.cmdType = CMD.STOP_STREAMJOB;
                    break;
                }
            }
            case DROP_STREAMJOB:{
                String regExDropStream = PATTERN_DROP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDropStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    this.cmdType = CMD.DROP_STREAMJOB;
                    break;
                }
            }
            /*case DESCRIBE_STREAMJOB: {
                String regExDescStream = PATTERN_DESCRIBE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDescStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    this.cmdType = cmdType;
                    break;
                } //else //do nothing
            }*/
            case CREATE_STREAM: {
                String regExCreateStream = PATTERN_CREATE_STREAM;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    cmd = "CREATE TABLE " + matcher.group(2) + "(a int)" + matcher.group(3);
                    this.cmdType = CMD.CREATE_STREAM;
                    break;
                }
            }
            case SHOW_STREAMS: {
                String regExCreateStream = PATTERN_SHOW_STREAMS;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    cmd = "SHOW TABLES";
                    this.cmdType = CMD.SHOW_STREAMS;
                    break;
                }
            }
            case DROP_STREAM: {
                String regExCreateStream = PATTERN_DROP_STREAM;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    cmd = cmd.replace(matcher.group(1), "DROP TABLE ");
                    this.cmdType = CMD.DROP_STREAM;
                    break;
                }
            }
            case INSERT_STREAM: {


                String regExInsertJoin = PATTERN_INSERT_STREAM_JOIN;
                Pattern patternInsertJoin = Pattern.compile(regExInsertJoin, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertJoin = patternInsertJoin.matcher(cmd);

                String regExInsertSth = PATTERN_INSERT_sth;
                Pattern patternInsertSth = Pattern.compile(regExInsertSth, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertSth = patternInsertSth.matcher(cmd);

                /*String regExInsertWin = PATTERN_INSERT_STREAM_WINDOW;
                Pattern patternInsertWin = Pattern.compile(regExInsertWin, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertWin = patternInsertWin.matcher(cmd);
                if (matcherInsertWin.matches()) {
                    String input = matcherInsertWin.group(2);
                    String output = matcherInsertWin.group(4);

                    if(getHiveVars() != null) {
                        Map<String, String> hiveVars = getHiveVars();
                        hiveVars.put("IS_STREAM_SQL", null);
                        hiveVars.put("INPUT_STREAMS", matcherInsertWin.group(4));
                        hiveVars.put("OUTPUT_STREAMS", matcherInsertWin.group(2));
                        hiveVars.put("RUN_TIME_TYPE", "output");
                        hiveVars.put("ORG_SQL", new String(cmd));
                        hiveVars.put("SUB_SELECT_SQL", new String(matcherInsertWin.group(3) + matcherInsertWin.group(4) + matcherInsertWin.group(5)));
                        Utility.Logger("INPUT_STREAMS:" + hiveVars.get("INPUT_STREAMS"));
                        Utility.Logger("OUTPUT_STREAMS:" + hiveVars.get("OUTPUT_STREAMS"));
                        Utility.Logger("SUB_SELECT_SQL:" + hiveVars.get("SUB_SELECT_SQL"));

                        String sql = "select 0 from " + input + "," + output + " limit 1";
                        this.cmd = sql;
                        this.cmdType = CMD.INSERT_STREAM;
                        break;
                    } else
                        throw new Exception("Get no space to cache variables!");
                } else */if (matcherInsertJoin.matches()) {
                    this.cmdType = CMD.INSERT_STREAM;
                    break;
                } else if (matcherInsertSth.matches()) {
                    this.cmdType = CMD.INSERT_STREAM;

                    // eg. insert into a select b;
                    ParseDriver pd = new ParseDriver();
                    ASTNode astNode = null;
                    try {
                        //TODO
                        astNode = (ASTNode) pd.parse(cmd).getChild(0);
                        if(astNode != null
                                && astNode.getToken().getType() == HiveParser.TOK_QUERY
                                && astNode.getChildCount() == 2
                                && astNode.getChild(0).getType() == HiveParser.TOK_FROM
                                && astNode.getChild(1).getType() == HiveParser.TOK_INSERT) {
                            //simple insert select
                            // eg. insert into a select b;
                            if(getHiveVars() != null) {
                                Map<String, String> hiveVars = getHiveVars();
                                StringBuilder inputStreams = new StringBuilder();
                                StringBuilder outputStreams = new StringBuilder();

                                getTableList(astNode.getChild(0).toStringTree(), inputStreams);
                                getTableList(astNode.getChild(1).toStringTree(), outputStreams);
                                hiveVars.put("IS_STREAM_SQL", null);
                                hiveVars.put("INPUT_STREAMS", inputStreams.toString().endsWith(",")
                                        ? inputStreams.toString().substring(0, inputStreams.toString().length() - 1)
                                        : inputStreams.toString());
                                hiveVars.put("OUTPUT_STREAMS", outputStreams.toString().endsWith(",")
                                        ? outputStreams.toString().substring(0, outputStreams.toString().length() -1 )
                                        : outputStreams.toString());
                                hiveVars.put("RUN_TIME_TYPE", "output");
                                hiveVars.put("ORG_SQL", new String(cmd));
                                hiveVars.put("SUB_SELECT_SQL", new String(matcherInsertSth.group(2)));
                                util.Logger("INPUT_STREAMS:" + inputStreams.toString());
                                util.Logger("OUTPUT_STREAMS:" + outputStreams.toString());
                                util.Logger("ORG_SQL:" + hiveVars.get("ORG_SQL"));

                                if(!outputStreams.toString().equals("")) {
                                    String sql = "select 0 from " + inputStreams + outputStreams.substring(0, outputStreams.length() - 1) + " limit 1";
                                    this.cmd = sql;
                                }
                            } else
                                throw new Exception("Get no space to cache variables!");
                            break;
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            case EXPLAIN_PLAN: {
                String regExExplain = PATTERN_EXPLAIN_PLAN;
                Pattern pattern = Pattern.compile(regExExplain, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    cmd = "select source, dest, runtimeType, sql from " + Conf.SYS_DB + ".relation";
                    this.cmdType = CMD.EXPLAIN_PLAN;
                    break;
                }
            }
            default: {
                this.cmdType = CMD.UNMATCHED;
                this.cmd = null;
                break;
            }
        }
    }
    public String getStreamJobName(){ return mapCmdParams.get(STREAM_JOB_NAME); }
    public String getStreamOutput(){ return mapCmdParams.get(STREAM_OUTPUT); }
    public String getStreamInput() { return mapCmdParams.get(STREAM_INPUT); }
    public String getTransformSql() { return this.cmd; }
    public CMD getCmdType() {return this.cmdType; }

    private Map<String, String> getHiveVars() {
        Map<String, String> hiveVars = null;
        SessionState ss = SessionState.get();
        if(ss != null)
            hiveVars = ss.getHiveVariables();

        return hiveVars;
    }

}
