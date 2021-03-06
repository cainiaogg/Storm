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
import java.text.SimpleDateFormat;
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
import java.util.concurrent.RunnableFuture;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Method.get;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import static org.apache.hadoop.hbase.protobuf.ResponseConverter.LOG;
import redis.clients.jedis.Jedis;
import scala.tools.nsc.Global;

/**
 * Created by jingwei on 16/6/6.
 */
class KafkaClient {
    private final Producer<String ,byte[]> producer;
    private final Properties props = new Properties();
    private final String topic;
    public gao gao;
    public KafkaClient(String brokerList,String topic){
        props.put("metadata.broker.list",brokerList);
        props.put("key.serializer.class","kafka.serializer.StringEncoder");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        producer = new Producer<String, byte[]>(new ProducerConfig(props));
        this.topic = topic;
        gao = new gao();
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
    public class gao implements  Runnable{
        public gao(){}
        public void run(){
            try {
                for (int i = 0; i < 3333; i++) {
                    send(Bytes.toBytes("接收到pv: 2016-06-16 14:58:46.084\t119.250.54.117\t865175023446614\t338c59b3-f4db-4b1a-95a1-a7a5d6258af7\t469513\t5902015829_PINPAI-CPC\t153442\t79660\t1\t0\t-13845336|0|1|2|4S1oMezTh9X2EeEGxa6LaP|61|null|bj|null\tPDPS000000056439\t1239141\tWAP\txxl\t-\t0\t-\t-\n" +
                            "接"));
                }
            }catch(Exception e){
                System.out.println(e);
            }
        }
    }

    public void destory(){
        producer.close();
    }
}

class KafkaConsumer{
    private final ConsumerConnector consumer;
    public String topic;
    public KafkaConsumer(String topic){
        Properties props = new Properties();
        this.topic = topic;
        props.put("zookeeper.connect","10.13.3.68:2181/kafka-yanbing3");
        props.put("group.id","jingwei_test3");
        props.put("zookeeper.session.timeout.ms","200");
        props.put("zookeeper.sync.time.ms","200");
        props.put("auto.offset.reset", "smallest");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }
    void consume(){
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            int flag = 0 ;
            String message = new String(iterator.next().message());
            for(int i = 0;i<message.length()-1;i++){
                char t1 = message.charAt(i+1);
                char t = message.charAt(i);
                if(t1 == '$'&&t=='\t'){
                    flag = 1;
                    System.out.println("接收到click: "+message);
//                    System.exit(0);
                    break;
                }
            }
            if(flag == 0)
            System.out.println("接收到pv: "+message);
        }
    }
}


public class HbaseTest {
    public static void main(String [] args)throws Exception {
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
//        String [] str = {"123","23"};
//        System.out.println(str.length);
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date date = sdf.parse("2016-06-13 11:58:31.374");
//        Date date1 = sdf.parse("2016-06-13 11:58:32");
//        System.out.println((new Date()).getTime());
//        System.out.println(new String(sdf.format(new Date())));
//
//        System.out.println(sdf.format(date1.getTime()+1000));
//        Jedis jedis;
//        jedis = new Jedis("10.210.228.84",6381);
//        jedis.set("test_jingwei","1");
//        System.out.println(jedis.get("test_jingwei"));


//            Date date = new Date();
//            System.out.println(date);
//        Date last =new Date();
//        KafkaClient kafkaClient = new KafkaClient("10.13.3.68:9092","sampleTopic");

//        Thread a = new Thread(kafkaClient.gao);
//        Thread b = new Thread(kafkaClient.gao);
//        Thread c = new Thread(kafkaClient.gao);
//        a.start();
//        b.start();
//        c.start();
//        for(int i = 0;i<100000;i++)
//        kafkaClient.send(Bytes.toBytes("接收到pv: 2016-06-16 14:58:46.084\t119.250.54.117\t865175023446614\t338c59b3-f4db-4b1a-95a1-a7a5d6258af7\t469513\t5902015829_PINPAI-CPC\t153442\t79660\t1\t0\t-13845336|0|1|2|4S1oMezTh9X2EeEGxa6LaP|61|null|bj|null\tPDPS000000056439\t1239141\tWAP\txxl\t-\t0\t-\t-\n" +
//                "接收到pv: 2016-06-16 14:58:48.395\t27.200.112.202\t__27.200.112.202_1466059856_0.84980600\t589ec13c-60bc-43c8-bcc3-59d3be3dbe91\t218337\t5489558819_PINPAI-CPC\t95997\t28000\t1\t0\t-1414509117|3|1|1|3rH8szcJddz0yJOOyWS7tW|31|news.sina.cn|bj|null\tPDPS000000057100\t825745\tWAP\txxl\t-\t0\t-\t-"));
//        a.join();b.join();c.join();
//        Date now =new Date();
//        System.out.println(now.getTime() - last.getTime());
             KafkaConsumer kafkaConsumer = new KafkaConsumer("pvclkTopic");
             kafkaConsumer.consume();
        }
    }

