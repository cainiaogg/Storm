package com.sina.app.bolt.util;

/**
 * Created by jingwei on 16/6/1.
 */
public class FormatLog {
    public static final int clickToHbaseBoltNum = 1;
    public static final int clickToKafkaBoltNum = 1;
    public static final int clickFailToKafkaBoltNum = 1;
    public static final int FailKafkaSpoutNum = 1;
    public static final int failClickParseBoltNum = 1;
    public static final int failClickToHbaseBoltNum = 1;
    public static final int failClickToKafkaBoltNum = 1;

    public static final String FaiKafkaSpoutZooKeeperList = "10.13.3.68:2181/kafka-yanbing3";

    public static final int hashMod = 100;
    public static final String brokerList = "10.13.3.68:9092";
    public static final String failbrokerList = "10.13.3.68:9092";
    public static final String pvTopic = "pvTopic";
    public static final String pvclkTopic = "pvclkTopic";
    public static final String failTopic = "failTopic";
    public static final int sampleCnt = 100; //chou yang
    public static final String tableFamily = "cf1"; //lie cu
    public static final int cntBatch = 2000; // batch count
    public static final int bufferLen = 30000; //huan chong qu chang du
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
