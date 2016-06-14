package com.sina.app.bolt.util;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.protobuf.ResponseConverter.LOG;

/**
 * Created by jingwei on 16/6/8.
 */
public class ClkWriteToHbase extends writeToHbase{
    public ClkWriteToHbase(String tableColumn){
        super(tableColumn);
    }
    @Override
    public void produce(String row,String val,String timeSign)throws InterruptedException{
        Get get = new Get(Bytes.toBytes(row));
        get.addColumn(Bytes.toBytes(tableFamily),Bytes.toBytes("logpv"));
        try{
            Result result = table.table.get(get);
            if(result.isEmpty());
            else{
                System.out.println(row);
            }
            Pair pair = new Pair(row ,val ,timeSign);
            buffer.put(pair);
        }catch(Exception e){
            LOG.error("logclk produce error {}",e);
        }
    }
}
