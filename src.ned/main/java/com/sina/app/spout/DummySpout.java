package com.sina.app.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Read log from file. For test usage.
 * @author xiaocheng1
 */
public class DummySpout implements IRichSpout {
    private static final long serialVersionUID = -1L;
    private static final Logger LOG = LoggerFactory.getLogger(DummySpout.class);
 
    protected transient SpoutOutputCollector _collector;
    private String _topic;
    private String _consumer_group_id;
    private BufferedReader reader;
    private String logFile;

    public DummySpout(String logFile) {
    	this.logFile = logFile;
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        // declare a single field tuple, containing the message as read from kafka
        declarer.declare(new Fields("bytes"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
        _collector = collector;

        LOG.info("kafka spout opened, reading from topic {}, using group {}", _topic, _consumer_group_id);
    	try {
			reader = new BufferedReader(new FileReader(logFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void close() {
        // reset state by setting members to null
        _collector = null;
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
    	try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	String line;
    	try {
			if ((line = reader.readLine()) != null) {
				_collector.emit(new Values((Object) line.getBytes()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void ack(final Object o) {
    	LOG.debug("ack {} ", o);
    }

    @Override
    public void fail(final Object o) {
    	LOG.debug("fail - {} ", o);
    }
}
