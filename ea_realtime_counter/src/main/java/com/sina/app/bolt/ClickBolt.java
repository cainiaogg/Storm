package com.sina.app.bolt;

import java.util.Map;

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
	
	private static final Logger LOG = LoggerFactory.getLogger(ClickBolt.class);
	private OutputCollector collector;
	
	public ClickBolt() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("adtype", "stattype", "psid", "infos",
				"order", "isClick", "timestamp"));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	/**
	 * There might be multiple click logs in one input!!
	 */
	public void execute(Tuple input) {
		String entry = new String((byte[]) input.getValue(0));
		// String[] clickLogs = StringUtils.splitPreserveAllTokens(entry, '\n');
//		System.out.println("*************");
//		System.out.println(entry);
//		System.out.println("*************");
		String[] clickLogs = StringUtils.split(entry, '\n');

		for (String oneLog: clickLogs) {
			ClickLog log = new ClickLog(oneLog);
			if (!log.isValid) {
				LOG.error("Wrong format of log: {}", oneLog);
				continue;
			}
			
			// filter cheating click
			if (log.isCheatClick()) {
				continue;
			}

			if (log.order == null) {
				LOG.error("Could not get order: {}", log.extField);
				continue;
			}
			
			// check platform type and idea type
			String adtype = ParserUtil.formatAdtype(log.platType, log.ideaType);
			if (adtype == null) {
				LOG.error("Invalid platType/ideaType: " + log.platType + "/" + log.ideaType);
				continue;
			}
			
			// emit intermediate data
			emitData(adtype, log.psid, log);
			// emit data for multi-channel positions
			if (log.channel != null) {
				emitData(adtype, log.psid + "+" + log.channel, log);
			}

		}
		collector.ack(input);
	}

	private void emitData(String adtype, String psid, ClickLog log) {

		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID,
					psid, "",
					log.order, 1, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_LINEITEM,
					psid, log.lineitem,
					log.order, 1, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_IDEA,
					psid, log.creative,
					log.order, 1, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_GROUP,
					psid, log.group,
					log.order, 1, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_CUST,
					psid, log.customer,
					log.order, 1, log.time));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
