package com.sina.app.bolt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Write result to file. For test usage.
 * @author xiaocheng1
 *
 */
public class LocalFileBolt implements IRichBolt {

	private static final long serialVersionUID = -8552441403541402088L;
	private static final Logger LOG = LoggerFactory.getLogger(LocalFileBolt.class);
	private OutputCollector collector;

	public LocalFileBolt() {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple input) {
		String key = input.getString(0);
		byte[] content = input.getBinary(1);
		LOG.info("Get tuple {} - {}", key, content);
		try {
			FileOutputStream stream = new FileOutputStream(new File(key));
			stream.write(content);
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
