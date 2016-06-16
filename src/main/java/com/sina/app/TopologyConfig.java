package com.sina.app;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.app.util.Constant;

public class TopologyConfig implements Serializable {

	private static final long serialVersionUID = -2865778476823885631L;
	private static final Logger LOG = LoggerFactory.getLogger(TopologyConfig.class);

	public String topologyName;

	public int clickSpoutNum;
	public int impressionSpoutNum;
	public int clickBoltNum;
	public int impressionBoltNum;
	public int intervalCountBoltNum;
	public int redisBoltNum;

	public int workerNum;
	public int maxPendingNum;
	public int ackNum;

	public String kafkaClickTopic;
	public String kafkaImpressionTopic;
	public String KafkaClickGroup;
	public String KafkaImpressionGroup;

	public String kafkaZookeeper;
	public int kafkaConsumerTimeoutMs;

	public String redisConfigList;

	public String maxRedisConn;
	public String maxRedisIdle;
	public String maxWaitMs;
	
	public int intervalCountSeconds;

	public String mailAlert;
	public String phoneAlert;

	public int transferBufferSize;
	
	public String alertKid;
	public String alertPassword;
	public String alertGroupName;
	public String alertServiceName;
	public String alertMailTo;
	public String alertMessageTo;
	
	public String alertCheckInterval;
	public String alertFailureThreshold;
	
	private Properties props;

	public TopologyConfig(Properties props) {
		this.props = props;
	}

	public void parse() {
		topologyName = get(Constant.TOPOLOGY_NAME_KEY);

		clickSpoutNum = getInt(Constant.CLICK_SPOUT_NUM);
		impressionSpoutNum = getInt(Constant.IMPRESSION_SPOUT_NUM);
		clickBoltNum = getInt(Constant.CLICK_BOLT_NUM);
		impressionBoltNum = getInt(Constant.IMPRESSION_BOLT_NUM);
		intervalCountBoltNum = getInt(Constant.INTERVAL_COUNT_BOLT_NUM);
		redisBoltNum = getInt(Constant.REDIS_BOLT_NUM);

		workerNum = getInt(Constant.WORKER_NUM_KEY);
		maxPendingNum = getInt(Constant.MAX_PENDING_KEY);
		ackNum = getInt(Constant.ACK_NUM_KEY);

		kafkaClickTopic = get(Constant.KAFKA_CLICK_TOPIC);
		kafkaImpressionTopic = get(Constant.KAFKA_IMPRESSION_TOPIC);
		KafkaClickGroup = get(Constant.KAFKA_CLICK_GROUP);
		KafkaImpressionGroup = get(Constant.KAFKA_IMPRESSION_GROUP);

		kafkaZookeeper = get(Constant.KAFKA_ZK_KEY);
		kafkaConsumerTimeoutMs = getInt(Constant.KAFKA_CONSUMER_TIMEOUT_KEY);

		redisConfigList = get(Constant.REDIS_LIST);
		
		intervalCountSeconds = getInt(Constant.INTERVAL_COUNT_SECONDS);

		transferBufferSize = getInt(Constant.TRANSFER_BUFFER_SIZE_KEY);
		
		alertKid = get(Constant.ALERT_KID);
		alertPassword = get(Constant.ALERT_PASSWORD);
		alertGroupName = get(Constant.ALERT_GROUP_NAME);
		alertServiceName = get(Constant.ALERT_SERVICE_NAME);
		alertMailTo = get(Constant.ALERT_MAIL_TO);
		alertMessageTo = get(Constant.ALERT_MSG_TO);
		
		alertCheckInterval = get(Constant.ALERT_CHECK_INTERVAL_SECONDS);
		alertFailureThreshold = get(Constant.ALERT_FAILURE_THRESHOLD);
	}

	public int getInt(String key) {
		return Integer.parseInt(props.getProperty(key));
	}

	public String get(String key) {
		return props.getProperty(key);
	}

	/**
	 * Convert props to XML string.
	 * @return
	 */
	public String getConfigXmlString() {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			props.storeToXML(stream, "Configuration of log format");
		} catch (IOException e1) {
			e1.printStackTrace();
			LOG.error("Failed to convert config to xml" + e1.getMessage());
		}
		return stream.toString();
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));
		TopologyConfig conf = new TopologyConfig(props);
		conf.parse();
	}
}
