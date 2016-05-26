package com.sina.app.bolt;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.sina.app.bolt.util.RedisPool;
import com.sina.app.metric.RedisMetricsConsumer;
import com.sina.app.util.Constant;

public class RedisBolt implements IRichBolt {

	private static final long serialVersionUID = -3572498870686887308L;
	private static final Logger LOG = LoggerFactory.getLogger(RedisBolt.class);
	private OutputCollector collector;
	private final int REDIS_EXPIRE_TIME = 86400 * 1;
	/**
	 * the key of key list
	 */
	private static final String REDIS_KEYLIST_ID = "adalgo_ea_rts_keylist";
//	private static final String REDIS_KEY_PREFIX = "adalgo_ea_rts_";
	/**
	 * max size of the buffer
	 */
	private static final int MAX_BUFFER_SIZE = 2000;
	/**
	 * max retry time of putting data to redis
	 */
	private static final int MAX_RETRY_TIME = 3;
	
	private LinkedBlockingQueue<Node> dataBuffer;
	private Thread writingThread;
	private RedisWriter writer;
	private RedisPool redisPool;
	
	private transient CountMetric countMetric;
	private int metricCheckInterval;

	public RedisBolt() {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			LOG.info("Load redis config: " + conf.get("redis.list"));
			redisPool = new RedisPool((String) conf.get("redis.list"));
		} catch (Exception e) {
			throw new RuntimeException("Failed to connect to redis: " + e);
		}
		
		metricCheckInterval = Integer.parseInt(String.valueOf(conf.get(Constant.ALERT_CHECK_INTERVAL_SECONDS)));

		countMetric = new CountMetric();
		context.registerMetric(RedisMetricsConsumer.REDIS_FAILURE_METRIC,
				countMetric, metricCheckInterval);
		
		dataBuffer = new LinkedBlockingQueue<Node>(MAX_BUFFER_SIZE);
		
		writer = new RedisWriter(redisPool);
		writingThread = new Thread(writer);
		writingThread.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple input) {
		String redisKey = input.getString(0);
		byte[] tableContent = input.getBinary(1);
//		boolean ret = dataBuffer.offer(new Node(redisKey, tableContent), 3, unit);
		boolean ret = dataBuffer.offer(new Node(redisKey, tableContent));
		if (ret) {
			collector.ack(input);
		} else {
			collector.fail(input);
			LOG.error("Buffer is full! Give up old data");
			// TODO: clear??
			// dataBuffer.clear();
			// TODO: should we call countMetric.incr()?? is it thread safe?
			dataBuffer.poll();
			dataBuffer.offer(new Node(redisKey, tableContent));
		}
	}

	@Override
	public void cleanup() {
		LOG.info("redis bolt clean up");
		writer.terminte();
		try {
			writingThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		redisPool.destroy();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	class Node {
		public String key;
		public byte[] value;
		public int retry = 0;
		
		public Node(String k, byte[] v) {
			key = k;
			value = v;
		}
	}
	
	/**
	 * 
	 * @author xiaocheng1
	 *
	 */
	class RedisWriter implements Runnable {
		
		RedisPool redisPool;
		private volatile boolean running = true;
		
		public RedisWriter(RedisPool pool) {
			redisPool = pool;
		}

		@Override
		public void run() {
			while (running) {
				Node node = null;
				try {
					node = dataBuffer.take();
				} catch (InterruptedException e) {
					LOG.info("Redis writer thread is been interrupted.");
					running = false;
				}
				if (node != null) {
					// store to redis
					boolean ret = redisPool.store(
							REDIS_KEYLIST_ID.getBytes(Charset.forName("UTF-8")),
							node.key.getBytes(Charset.forName("UTF-8")),
							node.value,
							REDIS_EXPIRE_TIME);
					if (!ret) {
						// retry
						if (++node.retry < MAX_RETRY_TIME) {
							if (dataBuffer.offer(node))
								continue;
						}
						LOG.error("Failed to store key {} after {} times, give up.", node.key, MAX_RETRY_TIME);
						countMetric.incr();
					}
				}
			}
		}

		public void terminte() {
			running = false;
		}
	}
}
