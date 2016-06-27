package com.sina.app.bolt.util;

import redis.clients.jedis.Jedis;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

/**
 * Created by jingwei on 16/6/7.
 */
public class TimeSign {
    private Jedis jedis;
    FormatLog formatLog = new FormatLog();
    public TimeSign(){
        try {
            jedis = new Jedis(formatLog.timeSignRedis, formatLog.timeSignRedisPort);
        }catch(Exception e){
            LOG.error("connect redis error{}",e);
        }
    }
    public String getTime()throws Exception{
        return jedis.get(formatLog.timeSignRedisKey);
    }
    public void updateTime(String tmpTime)throws Exception{
        jedis.set(formatLog.timeSignRedisKey,tmpTime);
    }
}
