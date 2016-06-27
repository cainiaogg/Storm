package com.sina.app.bolt.util;

/**
 * Created by jingwei on 16/6/1.
 */
public class FormatLog {
    public static   int clickToHbaseBoltNum = 1;
    public static   int clickToKafkaBoltNum = 1;
    public static   int clickFailToKafkaBoltNum = 1;
    public static   int FailKafkaSpoutNum = 1;
    public static   int failClickToHbaseBoltNum = 1;
    public static   int failClickToKafkaBoltNum = 1;

    public static   int cleanWriteToHbaseTimeout = 1000;
    public static   int batchWriteToHbaseTimeout = 1000;
//    public static   int impressionToKafkaBoltNum = 1;
    public static   String timeSignRedis = "127.0.0.1";
    public static   int timeSignRedisPort = 6379;
    public static   String timeSignRedisKey = "shixi_jingwei_time";

    public static   String FaiKafkaSpoutZooKeeperList = "10.13.3.68:2181/kafka-yanbing3";
    public static   int hashMod = 100;
    public static   String tableColumnClk = "logclk";
    public static   String tableColumnPv = "logpv";
    public static   String brokerList = "10.13.3.68:9092";
    public static   String failbrokerList = "10.13.3.68:9092";
    public static   String pvTopic = "pvTopic";
    public static   String pvclkTopic = "pvclkTopic";
    public static   String failTopic = "failTopic";
    public static   int sampleCnt = 100; //chou yang
    public static   String tableFamily = "cf1"; //lie cu
    public static   int cntBatch = 2000; // batch count
    public static   int bufferLen = 30000; //huan chong qu chang du

    public static   int firstAskHbaseTimeMax = 1000*60;
    public static   int firstAskHbaseTimeDlt = 1000*60*5;
    public static   int firstAskHbaseSleepTime = 1000*2;
    public static   int secondAskHbaseTimeMax = 1000*60;
    public static   int secondAskHbaseTimeDlt = 1000*60*5;
    public static   int secondAskHbaseSleepTime = 1000*2;


    public int getHash(String uuid){
        int valHash = 0;
        for(int i = 0;i < uuid.length();i++){
            char item = uuid.charAt(i);
            valHash = valHash*26 + item;
        }
        valHash %= hashMod;
        return valHash >= 0 ? valHash:(valHash + hashMod)%hashMod;
    }
}
