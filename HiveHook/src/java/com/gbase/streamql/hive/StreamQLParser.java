package com.gbase.streamql.hive;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.HashMap;

public class StreamQLParser {

    private String PATTERN_CREATE_STREAMJOB = "^([ ]*CREATE[ ]+STREAMJOB[ ]+)([a-zA-Z0-9]+)([ ]+TBLPROPERTIES[ ]*\\(\\\"jobdef\\\"=\\\")([a-zA-Z0-9/\\.]+)(\\\"\\)[ ]*)$";
    private String PATTERN_SHOW_STREAMJOBS = "^[ ]*SHOW[ ]+STREAMJOBS[ ]*$";
    private String PATTERN_START_STREAMJOB = "(^[ ]*start[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_STOP_STREAMJOB = "(^[ ]*stop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_DROP_STREAMJOB = "(^[ ]*drop[ ]+streamjob[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String PATTERN_DESCRIBE_STREAMJOB = "^([ ]*describe[ ]+)([a-zA-Z/0-9]+)([ ]*)$";
    private String PATTERN_CREATE_STREAM = "(^[ ]*CREATE[ ]+STREAM[ ]+)(.*)";
    private String PATTERN_SHOW_STREAMS = "^[ ]*SHOW[ ]+STREAMS[ ]*$";
    private String PATTERN_DROP_STREAM = "(^[ ]*drop[ ]+stream[ ]+)([a-zA-Z0-9]+)([ ]*)$";
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_JOB_DEF = "streamJobDef";
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
                    mapCmdParams.put(STREAM_JOB_DEF, matcher.group(4));
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
            default: {
                this.cmdType = cmdType;
                break;
            }
        }
    }
    public String getStreamJobName(){ return mapCmdParams.get(STREAM_JOB_NAME); }
    public String getStreamJobDef(){ return mapCmdParams.get(STREAM_JOB_DEF); }
    public String getTransformSql() { return this.cmd; }
    public CMD getCmdType() {return this.cmdType; }
}
