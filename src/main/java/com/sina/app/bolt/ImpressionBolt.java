package com.sina.app.bolt;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sina.app.bolt.util.writeToHbase;
import org.apache.commons.lang.StringUtils;
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
	public ImpressionBolt() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		write = new writeToHbase("logpv");
		ExecutorService service = Executors.newFixedThreadPool(2);
		service.submit(write.consumer);
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
				write.produce(log.uuid, log.logpvVal);
			}catch(InterruptedException e){
				LOG.error("produce error{}",e);
			}
		}
		collector.ack(input);
	}
	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
