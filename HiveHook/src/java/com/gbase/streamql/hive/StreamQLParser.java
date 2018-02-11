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
    private String STREAM_JOB_NAME = "streamJobName";
    private String STREAM_JOB_DEF = "streamJobDef";
    private Map<String, String> mapCmdParams = new HashMap<String, String>();
    private String cmd;

    public StreamQLParser (String cmd){
        this.cmd = cmd;
    }

    public CMD getCmdType() {
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
                    break;
                } else {
                    cmdType = CMD.SHOW_STREAMJOBS;
                }
            }
            case SHOW_STREAMJOBS: {
                String regExShowStream = PATTERN_SHOW_STREAMJOBS;
                Pattern pattern = Pattern.compile(regExShowStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    break;
                } else {
                    cmdType = CMD.START_STREAMJOB;
                }
            }
            case START_STREAMJOB: {
                String regExStartStream = PATTERN_START_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStartStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = CMD.STOP_STREAMJOB;
                }
            }
            case STOP_STREAMJOB: {
                String regExStopStream = PATTERN_STOP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExStopStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = CMD.DROP_STREAMJOB;
                }
            }
            case DROP_STREAMJOB:{
                String regExDropStream = PATTERN_DROP_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDropStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } else {
                    cmdType = CMD.DROP_STREAMJOB;
                }
            }
            /*case DESCRIBE_STREAMJOB: {
                String regExDescStream = PATTERN_DESCRIBE_STREAMJOB;
                Pattern pattern = Pattern.compile(regExDescStream, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(cmd);
                if (matcher.matches()) {
                    mapCmdParams.put(STREAM_JOB_NAME, matcher.group(2));
                    break;
                } //else //do nothing
            }*/

            default: {
                cmdType = CMD.UNMATCHED;
                break;
            }
        }

        return cmdType;
    }
    public String getStreamJobName(){
        return mapCmdParams.get(STREAM_JOB_NAME);
    }
    public String getStreamJobDef(){
        return mapCmdParams.get(STREAM_JOB_DEF);
    }
    private String getCmdParam(String keyName) {
        return mapCmdParams.get(keyName);
    }
}
