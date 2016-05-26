package com.sina.app.bolt;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import static org.mockito.Mockito.*;


public class RedisBoltTest {

	/**
	 * Check the failure strategy.
	 */
	public static void test() {
		RedisBolt bolt = new RedisBolt();
		Map conf = mock(Map.class);
		// use invalid redis, RedisPool.store() will return false.
		when(conf.get("redis.list")).thenReturn("invalid_host:9901");
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("redis_key");
		when(tuple.getBinary(1)).thenReturn("value".getBytes());
		for (int i = 0; i< 5; i++) {
			bolt.execute(tuple);
		}
		try {
			Thread.sleep(20000);	// sleep to see the log
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		test();
	}
}