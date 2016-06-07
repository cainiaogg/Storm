package com.sina.app.bolt.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
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
    public Pair(String row,String val){
        this.row = row;
        this.val = val;
    }
}

public class writeToHbase extends FormatLog{
    public String tableColumn;
    private OperateTable table;
    public BlockingQueue<Pair> buffer;
    public List<Pair>  writeList;
    public Consumer consumer;
    public KafkaClient kafkaClient;
    public writeToHbase(String tableCloumn){
        this.tableColumn = tableCloumn;
        kafkaClient =  new KafkaClient(brokerList,sampleTopic);
        consumer = new Consumer();
        table = new OperateTable();
        buffer = new LinkedBlockingDeque<Pair>(bufferLen);
        writeList = new ArrayList<Pair>();
    }
    public void batchWrite(){
        System.out.println("*********************");
        System.out.println(buffer.size());
        System.out.println("*********************");
        List<Put> putList = new ArrayList<Put>();
        for(Pair tmp:writeList){
            if(tableColumn == "logclk"){
                Get get = new Get(Bytes.toBytes(tmp.row));
                get.addColumn(Bytes.toBytes(tableFamily),Bytes.toBytes("logpv"));
                try{
                    Result result = table.table.get(get);
                    if(result.isEmpty()) continue;
                    String pv = "";
                    for(KeyValue kv:result.list()){
                        pv = new String(kv.getValue());
                    }
                    pv = pv + "\t$" + tmp.val;
                    kafkaClient.send(Bytes.toBytes(pv));
                }catch(Exception e){
                    LOG.error("get from hbase error {}",e);
                }
            }
            else
            kafkaClient.send(Bytes.toBytes(tmp.val));
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
    public void produce(String row ,String val) throws InterruptedException{
        Pair pair = new Pair(row,val);
        buffer.put(pair);
    }
    public Pair consume() throws InterruptedException{
        return buffer.take();
    }
    public class Consumer implements Runnable{
        public Consumer(){
        }
        public void run(){
            try{
                while(true){
                    if(writeList.size() >= cntBatch){
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
