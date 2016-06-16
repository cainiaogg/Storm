package com.sina.app.bolt.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.hadoop.fs.FileContext.LOG;

/**
 * Created by jingwei on 16/6/1.
 */
class Pair{
    public String row;
    public String val;
    public String timeSign;
    public Pair(String row,String val,String timeSign){
        this.row = row;
        this.val = val;
        this.timeSign = timeSign;
    }
}

public class writeToHbase extends FormatLog{
    public String tableColumn;
    public OperateTable table;
    public BlockingQueue<Pair> buffer;
    public List<Pair>  writeList;
    public Consumer consumer;
    public KafkaClient kafkaClient;
    public TimeSign timeSign;
    public boolean cleanUp;
    public writeToHbase(String tableCloumn){
        cleanUp = true;
        this.tableColumn = tableCloumn;
        consumer = new Consumer();
        table = new OperateTable();
        buffer = new LinkedBlockingDeque<Pair>(bufferLen);
        writeList = new ArrayList<Pair>();
        timeSign = new TimeSign();
    }
    public void batchWrite(){
        List<Put> putList = new ArrayList<Put>();
        String tmpTime = new String(writeList.get(writeList.size() - 1).timeSign);
        try {
            timeSign.updateTime(tmpTime);
        }catch(Exception e){
            LOG.error("error update redisTime{}",e);
        }
        for(Pair tmp:writeList){
            Put put = new Put(Bytes.toBytes(tmp.row));
            put.add(Bytes.toBytes(tableFamily),Bytes.toBytes(tableColumn),Bytes.toBytes(tmp.val));
            putList.add(put);
        }
        try {
            table.addRows(putList);
        }catch(Exception e){
            LOG.error("addRows error of {}",e);
        }
    }
    public void produce(String row ,String val,String timeSign) throws InterruptedException{
        Pair pair = new Pair(row,val,timeSign);
        buffer.put(pair);
    }
    public Pair consume() throws InterruptedException{
        return buffer.take();
    }
    public void clean(){
        Date last = new Date();
        while(true){
            Date now = new Date();
            if(now.getTime() - last.getTime() > 1000) break;
            if(buffer.isEmpty()) break;
        }
        cleanUp = false;
    }
    public class Consumer implements Runnable{
        public Consumer(){
        }
        public void run(){
            try{
                Date lastTime = new Date();
                while(cleanUp){
                    Date nowTime = new Date();
                    if(writeList.size() >= cntBatch && nowTime.getTime() - lastTime.getTime() > 1000){
                        batchWrite();
                        writeList.clear();
                    }
                    writeList.add(consume());
                }
            }catch(InterruptedException e){
                LOG.error("consume error of {}",e);
            }
        }
    }
}
