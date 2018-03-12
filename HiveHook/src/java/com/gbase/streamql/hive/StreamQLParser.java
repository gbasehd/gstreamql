package com.gbase.streamql.hive;

import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

public class StreamQLParser {

    private String PATTERN_CREATE_STREAMJOB = "^([ ]*CREATE[ ]+STREAMJOB[ ]+)([a-zA-Z0-9\\.]+)([ ]+TBLPROPERTIES[ ]*\\(\\\"output\\\"=\\\")([a-zA-Z0-9/\\.]+)(\\\"\\)[ ]*)$";
    private String PATTERN_SHOW_STREAMJOBS = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
    private String PATTERN_START_STREAMJOB = "(^[ ]*start[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_STOP_STREAMJOB = "(^[ ]*stop[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_DROP_STREAMJOB = "(^[ ]*drop[ ]+streamjob[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    private String PATTERN_DESCRIBE_STREAMJOB = "^([ ]*describe[ ]+)([a-zA-Z/0-9]+)([ ]*)$";
    private String PATTERN_CREATE_STREAM = "(^[ ]*CREATE[ ]+STREAM[ ]+)(.*)";
    private String PATTERN_SHOW_STREAMS = "^[ ]*SHOW[ ]+STREAMS[ ]*$";
    private String PATTERN_DROP_STREAM = "(^[ ]*drop[ ]+stream[ ]+)([a-zA-Z0-9\\.]+)([ ]*)$";
    //TODO
    private String PATTERN_INSERT_STREAM_WINDOW = "(^[ ]*insert[ ]+into[ ]+[a-zA-Z0-9\\.]+[ ]+select[ ]+.*from[ ]+[a-zA-Z0-9\\.]+[ ]+)(STREAMWINDOW[ ]+[a-zA-Z0-9\\.]+[ ]+as[ ]*.*)";
    private String PATTERN_INSERT_STREAM_JOIN = "insert stream join";
    private String PATTERN_INSERT_sth = "^[ ]*insert[ ]+into[ ]+[a-zA-Z0-9\\.]+[ ]+select.*";
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_OUTPUT = "streamOutput";

    private CMD cmdType;
    private Map<String, String> mapCmdParams = new HashMap<String, String>();
    private String cmd;

    public StreamQLParser (String cmd){
        this.cmd = cmd;
    }

    public void parse() {
        //init cmd type
        CMD cmdType = CMD.CREATE_STREAMJOB;
        switch(cmdType) {
            case CREATE_STREAMJOB: {
                String regExCreateStream = PATTERN_CREATE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExCreateStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if(matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    mapCmdParams.put(STREAM_OUTPUT, matcher.group(4));
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
                    cmd = cmd.replace(matcher.group(1), "CREATE TABLE ");
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
                String regExInsertWin = PATTERN_INSERT_STREAM_WINDOW;
                Pattern patternInsertWin = Pattern.compile(regExInsertWin, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertWin = patternInsertWin.matcher(cmd);

                String regExInsertJoin = PATTERN_INSERT_STREAM_JOIN;
                Pattern patternInsertJoin = Pattern.compile(regExInsertJoin, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertJoin = patternInsertJoin.matcher(cmd);

                String regExInsertSth = PATTERN_INSERT_sth;
                Pattern patternInsertSth = Pattern.compile(regExInsertSth, Pattern.CASE_INSENSITIVE);
                Matcher matcherInsertSth = patternInsertSth.matcher(cmd);

                if (matcherInsertWin.matches()) {
                    cmd = matcherInsertWin.group(1);
                    this.cmdType = CMD.INSERT_STREAM;
                    break;
                } else if (matcherInsertJoin.matches()) {
                    this.cmdType = CMD.INSERT_STREAM;
                    break;
                } else if (matcherInsertSth.matches()) {
                    this.cmdType = CMD.INSERT_STREAM;

                    // eg. insert into a select b;
                    ParseDriver pd = new ParseDriver();
                    ASTNode astNode = null;
                    try {
                        astNode = (ASTNode) pd.parse(cmd).getChild(0);
                        if(astNode != null
                                && astNode.getToken().getType() == HiveParser.TOK_QUERY
                                && astNode.getChildCount() == 2
                                && astNode.getChild(0).getType() == HiveParser.TOK_FROM
                                && astNode.getChild(1).getType() == HiveParser.TOK_INSERT) {
                            //simple insert select
                            // eg. insert into a select b;
                            SessionState ss = SessionState.get();
                            Map<String, String> hiveVars = ss.getHiveVariables();

                            StringBuilder inputStreams = new StringBuilder();
                            StringBuilder outputStreams = new StringBuilder();
                            //simple insert select
                            // eg. insert into a select b;

                            //input
                            getTableList(astNode.getChild(0).toStringTree(), inputStreams);
                            getTableList(astNode.getChild(1).toStringTree(), outputStreams);
                            hiveVars.put("IS_STREAM_SQL", null);
                            hiveVars.put("INPUT_STREAMS", inputStreams.toString());
                            hiveVars.put("OUTPUT_STREAMS", outputStreams.toString());
                            hiveVars.put("RUN_TIME_TYPE", "OUTPUT_STREAM");
                            hiveVars.put("ORG_SQL", new String(cmd));
                            Utility.Logger("INPUT_STREAMS:" + inputStreams.toString());
                            Utility.Logger("OUTPUT_STREAMS:" + outputStreams.toString());
                            Utility.Logger("ORG_SQL:" + hiveVars.get("ORG_SQL"));

                            if(!outputStreams.toString().equals("")) {
                                String sql = "select 0 from " + inputStreams + outputStreams.substring(0, outputStreams.length() - 1) + " limit 1";
                                this.cmd = sql;
                            }
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }





                    break;
                }
            }
            default: {
                this.cmdType = cmdType;
                break;
            }
        }
    }
    public String getStreamJobName(){ return mapCmdParams.get(STREAM_JOB_NAME); }
    public String getStreamOutput(){ return mapCmdParams.get(STREAM_OUTPUT); }
    public String getTransformSql() { return this.cmd; }
    public CMD getCmdType() {return this.cmdType; }


    /**
     * 递归截取字符串获取表名
     * @param strTree
     * @return
     */
    private void getTableList(String strTree, StringBuilder tabNames){
        int i1 = strTree.indexOf("tok_tabname");
        String substring1 = "";
        String substring2 = "";
        if(i1>0){
            substring1 = strTree.substring(i1+12);
            int i2 = substring1.indexOf(")");
            substring2 = substring1.substring(0,i2);
            System.out.println(substring2);
            tabNames.append(substring2).append(",");
            getTableList(substring1, tabNames);
        }
    }

}
