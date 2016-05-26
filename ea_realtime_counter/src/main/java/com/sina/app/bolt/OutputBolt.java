package com.sina.app.bolt;

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

import com.sina.app.bolt.util.KafkaClient;
import com.sina.app.metric.OutputMetricsConsumer;
import com.sina.app.util.Constant;

public class OutputBolt implements IRichBolt {

	private static final long serialVersionUID = -3572498870686887308L;
	private static final Logger LOG = LoggerFactory.getLogger(OutputBolt.class);
	private OutputCollector collector;

	/**
	 * max size of the buffer
	 */
	private static final int MAX_BUFFER_SIZE = 2000;
	/**
	 * max retry time of putting data to kafka
	 */
	private static final int MAX_RETRY_TIME = 1;
	
	private LinkedBlockingQueue<Node> dataBuffer;
	private Thread writingThread;
	private OutputWriter writer;

	private KafkaClient kafkaClient;
	
	private transient CountMetric countMetric;
	private int metricCheckInterval;

	public OutputBolt() {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try {
			String brokerList = (String) conf.get(Constant.OUTPUT_KAFKA_BROKER_LIST);
			String topic = (String) conf.get(Constant.OUTPUT_KAFKA_TOPIC);
			LOG.info("Load output kafka config: brokerList=" + brokerList + 
					",topic=" + topic);
			kafkaClient = new KafkaClient(brokerList, topic);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to connect to output kafka: " + e);
		}
		
		metricCheckInterval = Integer.parseInt(String.valueOf(conf.get(Constant.ALERT_CHECK_INTERVAL_SECONDS)));

		countMetric = new CountMetric();
		context.registerMetric(OutputMetricsConsumer.FAILURE_METRIC,
				countMetric, metricCheckInterval);
		
		dataBuffer = new LinkedBlockingQueue<Node>(MAX_BUFFER_SIZE);
		
		writer = new OutputWriter(kafkaClient);
		writingThread = new Thread(writer);
		writingThread.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple input) {
		String key = input.getString(0);
		byte[] tableContent = input.getBinary(1);
//		boolean ret = dataBuffer.offer(new Node(key, tableContent), 3, unit);
		boolean ret = dataBuffer.offer(new Node(key, tableContent));
		if (ret) {
			collector.ack(input);
		} else {
			collector.fail(input);
			LOG.error("Buffer is full! Give up old data");
			// TODO: clear??
			// dataBuffer.clear();
			// TODO: should we call countMetric.incr()?? is it thread safe?
			dataBuffer.poll();
			dataBuffer.offer(new Node(key, tableContent));
		}
	}

	@Override
	public void cleanup() {
		LOG.info("output bolt clean up");
		writer.terminte();
		try {
			writingThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		kafkaClient.destroy();
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
	 * Writing thread.
	 */
	class OutputWriter implements Runnable {
		
		KafkaClient kafkaClient;
		private volatile boolean running = true;
		
		public OutputWriter(KafkaClient client) {
			kafkaClient = client;
		}

		@Override
		public void run() {
			while (running) {
				Node node = null;
				try {
					node = dataBuffer.take();
				} catch (InterruptedException e) {
					LOG.info("Writer thread is been interrupted.");
					running = false;
				}
				if (node != null) {
					// send to kafka
					boolean ret = kafkaClient.send(node.key, node.value);
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
