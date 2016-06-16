package com.sina.app.bolt;

import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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
	private Thread writeThread;
	private ClickLog.ClkWriteToHbase write;
	public ClickBolt() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("GETK",new Fields("toKafka"));
			declarer.declareStream("GETH",new Fields("toHbase"));
			declarer.declareStream("NOGET",new Fields("toKafka"));
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
					write = new ClickLog.ClkWriteToHbase("logclk");
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
		String [] clickLogs = StringUtils.split(entry,"\n");
		for(String oneLog:clickLogs){
			ClickLog log = new ClickLog(oneLog);
			try {
				boolean askExist = write.askExist(log.uuid,log.logclkVal,log.timeSign);
				if(!askExist){
					collector.emit("NOGET",new Values(oneLog));
				}
				else{
					boolean askClk = write.askClk(log.uuid,log.logclkVal,log.timeSign);
					if(askClk) continue;
					collector.emit("GETK",new Values(write.pvFromHbase+"\t$\t"+log.logclkVal));
					collector.emit("GETH",new Values(oneLog));
				}
			}catch(Exception e){
				LOG.error("click produce error {}",e);
			}
		}
		collector.ack(input);
	}
	@Override
	public void cleanup() {
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
