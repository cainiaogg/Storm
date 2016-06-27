package com.sina.app.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.sina.app.bolt.util.ClickLog;
import com.sina.app.bolt.util.FormatLog;
import com.sina.app.bolt.util.writeToHbase;
import org.apache.hadoop.security.UserGroupInformation;


import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hbase.ipc.RpcClient.LOG;

import java.text.Normalizer;
import java.util.Date;
import java.util.Map;
/**
 * Created by jingwei on 16/6/14.
 */
public class ClickToHbaseBolt implements IRichBolt {
    private writeToHbase write;
    private Thread writeThread;
    private OutputCollector collector;
    private FormatLog formatlog = new FormatLog();
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        Object ret = null;
        try{
            ret = UserGroupInformation.createRemoteUser("hero").doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception{
                    write = new writeToHbase(formatlog.tableColumnClk);
                    writeThread = new Thread(write.consumer);
                    writeThread.start();
                    return null;
                }
            });
        }catch(Exception e){
            LOG.error("send userImf error {}",e);
        }
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input){
        String entry = new String(input.getValue(0).toString());
        ClickLog log = new ClickLog(entry);
        try {
            write.produce(log.uuid, log.logclkVal, log.timeSign);
        }catch(InterruptedException e){
            LOG.error("write clk to hbase error!{}",e);
            collector.fail(input);
        }
        collector.ack(input);
    }
    @Override
    public void cleanup() {
        write.clean();
        try{
            writeThread.join();
        }catch(InterruptedException e){
            LOG.error("write thread join error{}",e);
        }
    }
    @Override
    public java.util.Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
