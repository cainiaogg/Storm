package com.sina.app.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.sina.app.bolt.util.FormatLog;
import com.sina.app.bolt.util.KafkaClient;
import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import sun.awt.windows.ThemeReader;

import java.util.Map;

/**
 * Created by jingwei on 16/6/14.
 */
public class ToKafkaBolt implements IRichBolt {
    private KafkaClient kafkaClient;
    private OutputCollector collector;
    public String brokerList;
    public String pvclkTopic;
    public ToKafkaBolt(String brokerList,String pvclkTopic){
        this.brokerList = brokerList;
        this.pvclkTopic = pvclkTopic;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector){
        kafkaClient = new KafkaClient(brokerList,pvclkTopic);
    }
    @Override
    public void execute(Tuple input){
        byte[] entry = (byte[]) input.getValue(0);
        if(kafkaClient.send(entry))
            collector.ack(input);
        else collector.fail(input);
    }
    @Override
    public void cleanup(){
        kafkaClient.destory();
    }
    @Override
    public java.util.Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
