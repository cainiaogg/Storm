package com.sina.app.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.sina.app.bolt.util.Counter;
import com.sina.app.util.TupleHelpers;

import ea.proto.TableOuterClass.Record;
import ea.proto.TableOuterClass.Table;

public class IntervalCountBolt implements IRichBolt {
	
	private static final long serialVersionUID = 7548165236131884840L;
	private static final Logger LOG = LoggerFactory.getLogger(IntervalCountBolt.class);
	private OutputCollector collector;
	private int tickFrequencyInSeconds;
	
	private Counter counter;
	
	/**
	 * Initialize the tick config.
	 *
	 * NOTE: we could not initialize tickFrequencyInSeconds in prepare(), because
	 * getComponentConfiguration() is called before prepare().
	 * @param tickFrequencyInSeconds
	 * @return 
	 */
	public IntervalCountBolt(int tickFrequencyInSeconds) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
		LOG.info("init tick freq=" + tickFrequencyInSeconds);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counter = new Counter();
	}

	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("Received tick tuple, triggering emit of current window counts");
			emitCurrentWindowCounts();
		} else {
			countAndAck(tuple);
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// redisKey: String
		// table: byte[]
		declarer.declare(new Fields("redisKey", "table"));
	}
	
	private void emitCurrentWindowCounts() {
		for (Table table: counter.getCountingResult()) {
			collector.emit(new Values(getRedisKey(table), table.toByteArray()));
		}
		counter.clear();
	}
	
	private String getRedisKey(Table table) {
		String stattype = table.getStattype();
		long timestamp = table.getTs();
		Record record = table.getData(0);
		StringBuilder builder = new StringBuilder();
		builder.append(timestamp).append('~')
			.append(stattype).append('~')
			.append(record.getPsid()).append('~')
			.append(record.getInfos()).append('~')
			.append(record.getOrder());
		return builder.toString();
	}

	private void countAndAck(Tuple tuple) {
		String platIdeaType = tuple.getString(0);
		String statType = tuple.getString(1);
		String psid = tuple.getString(2);
		String infos = tuple.getString(3);
		String order = tuple.getString(4);
		int isClick = tuple.getInteger(5);
//		String timestamp = tuple.getString(6);

		// add count
		counter.increment(platIdeaType, statType, psid, infos, order, isClick);
	}

	@Override
	public void cleanup() {
		if (counter.hasData()) {
			LOG.info("Still has data before shutdown, start to commit");
			emitCurrentWindowCounts();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		// set up tick tuple
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

}
