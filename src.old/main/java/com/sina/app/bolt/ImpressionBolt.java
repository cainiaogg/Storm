package com.sina.app.bolt;

import java.io.FileOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.sina.app.bolt.util.writeToHbase;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.sina.app.bolt.util.ImpressionLog;

public class ImpressionBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ImpressionBolt.class);
	private OutputCollector collector;
	private writeToHbase write;
	private Thread writeThread;
	public ImpressionBolt() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("toKafka"));
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
					write = new writeToHbase("logpv");
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
		String entry = new String((byte[]) input.getValue(0));
		String [] impressionLogs = StringUtils.split(entry,"\n");
		for(String oneLog:impressionLogs){
			ImpressionLog log = new ImpressionLog(oneLog);

			if(!log.isValid){
//				LOG.error("Wrong format of log: {}",oneLog);
				continue;
			}
			try {
				write.produce(log.uuid, log.logpvVal,log.timeSign);
				collector.emit(new Values(oneLog));
			}catch(InterruptedException e){
				LOG.error("produce error{}",e);
			}
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
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
