package com.sina.app.bolt;

import java.util.Map;

import com.sina.app.bolt.util.OperateTable;
import org.apache.commons.httpclient.methods.ExpectContinueMethod;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.sina.app.bolt.util.ClickLog;
import com.sina.app.bolt.util.ParserUtil;

public class ClickBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static final String tableName = "sinaad_rtlabel";
	private static final String tableCloumn = "logclk";
	private static final String[] tableFamily ={
			"cf"
	};
	private static final Logger LOG = LoggerFactory.getLogger(ClickBolt.class);
	private OutputCollector collector;
	public OperateTable clickTable;

	public ClickBolt() {
		try {
			clickTable.createTable(tableName, tableFamily);
		}catch(Exception e){
			LOG.error("createTable error{}",e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	public void writeToHbase(ClickLog log){
		try{
			clickTable.addRow(tableName,log.uuid,tableFamily[0],tableCloumn,log.logclkVal);
		}catch (Exception e){
			LOG.error("addRow error:{}",e);
		}
	}
	@Override
	public void execute(Tuple input) {
		String entry = new String((byte[]) input.getValue(0));
		String[] clickLogs = StringUtils.split(entry, '\n');

		for (String oneLog: clickLogs) {
			ClickLog log = new ClickLog(oneLog);
			if (!log.isValid) {
				LOG.error("Wrong format of log: {}", oneLog);
				continue;
			}
			writeToHbase(log);
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
