package com.sina.app.bolt;

import java.io.FileOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sina.app.bolt.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


public class ClickBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ClickBolt.class);
	private OutputCollector collector;
	private clkWriteToHbase write;
	public ClickBolt() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
					write = new clkWriteToHbase("logclk");
					ExecutorService service = Executors.newFixedThreadPool(1);
					service.submit(write.consumer);
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
		String [] clickLogs = StringUtils.split(entry,"\n");
		for(String oneLog:clickLogs){
			ClickLog log = new ClickLog(oneLog);
			if(!log.isValid){
//				LOG.error("Wrong format of log: {}",oneLog);
				continue;
			}
			while(true){
				TimeSign timeSign = new TimeSign();
				if(log.timeSign.compareTo(timeSign.timeSign) >= 0) break;
			}
			try {
				write.produce(log.uuid, log.logclkVal, log.timeSign);
			}catch(Exception e){
				LOG.error("click produce error {}",e);
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
