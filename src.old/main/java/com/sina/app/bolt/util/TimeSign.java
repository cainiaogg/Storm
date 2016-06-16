package com.sina.app.bolt.util;

import redis.clients.jedis.Jedis;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

/**
 * Created by jingwei on 16/6/7.
 */
public class TimeSign {
    private Jedis jedis;
    public TimeSign(){
        try {
            jedis = new Jedis("127.0.0.1", 6379);
        }catch(Exception e){
            LOG.error("connect redis error{}",e);
        }
    }
    public String getTime()throws Exception{
        return jedis.get("shixi_jingwei_time");
    }
    public void updateTime(String tmpTime)throws Exception{
        jedis.set("shixi_jingwei_time",tmpTime);
    }
}
