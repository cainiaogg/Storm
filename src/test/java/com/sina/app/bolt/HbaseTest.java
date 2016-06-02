package com.sina.app.bolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Created by jingwei on 16/5/26.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sina.app.bolt.OperateTable;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;
class gao implements Runnable{
    public gao(){

    }
    public void run(){
        try{
            while(true){
                System.out.println("jijij");
                Thread.sleep(5000);
            }
        }catch(InterruptedException e){
            System.out.println(e);
        }
    }
}
public class HbaseTest{

    public static void main(String[] args){
        ExecutorService service = Executors.newFixedThreadPool(2);
        HbaseTest a = new HbaseTest();
        gao tt = new gao();
        service.submit(tt);
        System.out.println("nihao");
    }
}
