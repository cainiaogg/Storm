package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ImpressionLog{
    public String [] strLog = {
            "timestamp",
            "ip",
            "cookie",
            "pageUrl",
            "unitId",
            "uuid",
            "lineitemId",
            "adId",
            "userTagList",
            "targetTagList",
            "customerId",
            "sspId",
            "groupId",
            "price",
            "maxprice",
            "costType",
            "zone",
            "userAgent",
            "abTestName",
            "abTestValueList",
            "algoLogStr",
            "platform",
            "adType",
            "blogArticleId",
            "blogUserId",
            "useRetargeting",
    };
    public boolean isValid = true;
    public int impressionLength = 26;
    public String[] logValue;
    public String uuid;
    public ImpressionLog(String log) {
        String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
        if (segs.length != impressionLength) {
            isValid = false;
            return;
        }
        uuid = segs[5];
        for (int i = 0; i < 17; i++) {
            logValue[i] = segs[i];
        }

    }
}
