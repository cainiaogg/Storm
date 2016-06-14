package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

public class ImpressionLog extends FormatLog{
    public boolean isValid = true;
    public int impressionLength = 26;
    public String uuid;
    public String logpvVal;
    public String tableCloumn = "logpv";
    public String timeSign;
    public ImpressionLog(String log) {
        String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
        if (segs.length < impressionLength) {
            isValid = false;
            return;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            Date date = simpleDateFormat.parse(segs[0]);
        }catch(Exception e){
            isValid = false;
            LOG.error("ImpressionLog isValid",e);
            return ;
        }
        uuid = segs[5];
        timeSign = segs[0];
        int valHash = getHash(uuid);
        if(valHash >= sampleCnt){
            isValid = false;
            return ;
        }
        logpvVal = log;
    }
}
