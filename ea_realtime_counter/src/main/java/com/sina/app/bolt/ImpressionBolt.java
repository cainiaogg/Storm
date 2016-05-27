package com.sina.app.bolt;

import java.util.Map;

import com.sina.app.bolt.util.OperateTable;
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

import com.sina.app.bolt.util.ImpressionLog;
import com.sina.app.bolt.util.ParserUtil;

public class ImpressionBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private static final String tableName = "IC_LOG";
	private static final String familyCloumn = "impressionLog";
	private static final Logger LOG = LoggerFactory.getLogger(ImpressionBolt.class);
	private OutputCollector collector;
	private static final String[] tableFamily ={
			"clickLog",
			"impressionLog",
	};
	public ImpressionBolt() {
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
	public void writeToHbase(ImpressionLog log){
		OperateTable impressionTable = new OperateTable();
		try {
			impressionTable.createTable(tableName, tableFamily);
		}catch(Exception e){
			LOG.error("createTable error{}",e);
		}
		for(int i = 0;i<log.impressionLength;i++) {
			try {
				impressionTable.addRow(tableName, log.uuid, familyCloumn, log.strLog[i], log.logValue[i]);
			}catch(Exception e){
				LOG.error("addRow error:{}",e);
			}
		}
	}

	@Override
	public void execute(Tuple input) {
		String entry = new String((byte[]) input.getValue(0));
		String[] impressionLogs = StringUtils.split(entry, '\n');

		for (String oneLog: impressionLogs) {
			ImpressionLog log = new ImpressionLog(oneLog);
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
