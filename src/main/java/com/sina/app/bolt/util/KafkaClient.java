package com.sina.app.bolt.util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

import static org.apache.hadoop.hbase.protobuf.ResponseConverter.LOG;

/**
 * Created by jingwei on 16/6/6.
 */
public class KafkaClient {
    private final Producer<String ,byte[]> producer;
    private final Properties props = new Properties();
    private final String topic;
    public KafkaClient(String brokerList,String topic){
        props.put("metadata.broker.list",brokerList);
        props.put("key.serializer.class","kafka.serializer.StringEncoder");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        producer = new Producer<String, byte[]>(new ProducerConfig(props));
        this.topic = topic;
    }
    public boolean send(byte[] message){
        try{
            producer.send(new KeyedMessage<String, byte[]>(topic,message));
        } catch (kafka.common.MessageSizeTooLargeException e) {
            LOG.error("Message too large: len=" + message.length);
        } catch (kafka.common.FailedToSendMessageException e) {
            LOG.error("Failed to send: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to send: " + e.getMessage());
        }
        return  false;
    }
    public void destory(){
        producer.close();
    }
}
