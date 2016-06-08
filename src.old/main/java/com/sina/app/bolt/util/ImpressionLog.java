package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ImpressionLog extends FormatLog{
    public boolean isValid = true;
    public int impressionLength = 26;
    public String uuid;
    public String logpvVal;
    public String tableCloumn = "logpv";
    public ImpressionLog(String log) {
        String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
        if (segs.length < impressionLength) {
            isValid = false;
            return;
        }
        uuid = segs[5];
        int valHash = getHash(uuid);
        if(valHash >= sampleCnt){
            isValid = false;
            return ;
        }
        logpvVal = log;
    }
}
