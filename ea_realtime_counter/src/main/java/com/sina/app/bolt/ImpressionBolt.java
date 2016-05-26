package com.sina.app.bolt;

import java.util.Map;

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
	
	private static final Logger LOG = LoggerFactory.getLogger(ImpressionBolt.class);
	private OutputCollector collector;
	
	public ImpressionBolt() {
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
	public void execute(Tuple input) {
		String entry = new String((byte[]) input.getValue(0));
		process(entry);
		collector.ack(input);
	}
	
	private void process(String entry) {
		ImpressionLog log = new ImpressionLog(entry);
		if (!log.isValid) {
			LOG.error("Wrong format of log: {}", entry);
			return;
		}

		if (log.order == null) {
			// LOG.error("Could not get order: {}", log.algoExtField);
			return;
		}
		
		// check platform type and idea type
		String adtype = ParserUtil.formatAdtype(log.platType, log.ideaType);
		if (adtype == null) {
			LOG.error("Invalid platType/ideaType: " + log.platType + "/" + log.ideaType);
			return;
		}
		
		// emit intermediate data
		emitData(adtype, log.psid, log);
		// emit for multi-channel positions
		if (log.channel != null) {
			emitData(adtype, log.psid + "+" + log.channel, log);
		}

	}

	private void emitData(String adtype, String psid, ImpressionLog log) {

		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID,
						psid, "",
						log.order, 0, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_LINEITEM,
						psid, log.lineitem,
						log.order, 0, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_IDEA,
						psid, log.creative,
						log.order, 0, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_GROUP,
						psid, log.group,
						log.order, 0, log.time));
		collector.emit(
				new Values(adtype, ParserUtil.STAT_TYPE_PSID_CUST,
						psid, log.customer,
						log.order, 0, log.time));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
