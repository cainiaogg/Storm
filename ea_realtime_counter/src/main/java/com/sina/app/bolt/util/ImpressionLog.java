package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ImpressionLog{
    public boolean isValid = true;
    public int impressionLength = 26;
    public String uuid;
    public String logpvVal;
    public ImpressionLog(String log) {
        String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
        if (segs.length < impressionLength) {
            isValid = false;
            return;
        }
        uuid = segs[5];
        logpvVal = log;
    }
}
