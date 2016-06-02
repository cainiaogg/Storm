package com.sina.app.bolt.util;

/**
 * Created by jingwei on 16/6/1.
 */
public class FormatLog {
    public static final int hashMod = 100;
    public static final int sampleCnt = 100; //chou yang
    public static final String tableFamily = "cf"; //lie cu
    public static final int cntBatch = 10; // batch count
    public static final int bufferLen = 2000; //huan chong qu chang du
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
