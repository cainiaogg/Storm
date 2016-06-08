package com.sina.app.bolt;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sina.app.bolt.OperateTable;
import org.apache.hadoop.ipc.UnexpectedServerException;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import com.sun.security.auth.callback.TextCallbackHandler;
import java.io.File;
import javax.security.auth.Subject;
import java.security.PrivilegedAction;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Method.get;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import static org.apache.hadoop.hbase.protobuf.ResponseConverter.LOG;

/**
 * Created by jingwei on 16/6/6.
 */
class KafkaClient {
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

class KafkaConsumer{
    private final ConsumerConnector consumer;
    public KafkaConsumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect","10.13.3.68:2181/kafka-yanbing3");
        props.put("group.id","jingwei_test");
        props.put("zookeeper.session.timeout.ms","200");
        props.put("zookeeper.sync.time.ms","200");
        props.put("auto.offset.reset", "smallest");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }
    void consume(){
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("sampleTopic", new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get("sampleTopic").get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            int flag = 0 ;
            String message = new String(iterator.next().message());
            for(int i = 0;i<message.length()-1;i++){
                char t1 = message.charAt(i+1);
                char t = message.charAt(i);
                if(t1 == '$'){
                    flag = 1;
                    System.out.println("接收到click: "+message);
//                    System.exit(0);
                    break;
                }
            }
//            if(flag == 0)
//            System.out.println("接收到pv: "+message);
        }
    }
}

class gao{
    public static int a = 10;
}


public class HbaseTest {
    public static void main(String [] args) throws Exception{
//        FileOutputStream out = new FileOutputStream("/Users/jingwei/test/Storm/ea_realtime_counter/log",true);
//        out.write("nihao".getBytes("utf-8"));
//        out.close();
//        OperateTable ss = new OperateTable();
//        HTable table = new HTable(ss.conf,"sinaad_rtlabel");
//        Get get = new Get(Bytes.toBytes("050cf0ff-9b5a-4a67-bb15-960e03eeb79a"));
//        get.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("logclk"));
//        try{
//            Result result= table.get(get);
//            if(result.isEmpty()) System.out.println("jiji");
//            for(KeyValue kv:result.list()){
//                System.out.println(new String(kv.getValue()));
//            }
//        }catch (Exception e){
//            System.out.println(e);
//        }
        gao a = new gao();
        a.a =11;
        gao b =  new gao();
        System.out.println(b.a);

        System.out.println(a.a);
//        KafkaClient kafkaClient = new KafkaClient("10.13.3.68:9092","sampleTopic");
//        kafkaClient.send(Bytes.toBytes("***************"));
//        KafkaConsumer kafkaConsumer = new KafkaConsumer();
//        kafkaConsumer.consume();
        }
    }

