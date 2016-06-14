package com.sina.app.bolt.util;

import redis.clients.jedis.Jedis;

/**
 * Created by jingwei on 16/6/7.
 */
public class TimeSign {
    private Jedis jedis;
    public TimeSign(){
        jedis = new Jedis("10.210.228.84",6381);
    }
    public String getTime(){
        return jedis.get("shixi_jingwei_time");
    }
    public void updateTime(String tmpTime){
        jedis.set("shixi_jingwei_time",tmpTime);
    }
}
