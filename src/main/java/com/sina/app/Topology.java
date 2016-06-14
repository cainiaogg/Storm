package com.sina.app;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.sina.app.bolt.ClickBolt;
import com.sina.app.bolt.ImpressionBolt;
import com.sina.app.metric.OutputMetricsConsumer;
import com.sina.app.spout.KafkaSpout;
import com.sina.app.util.Constant;
import com.sina.app.util.StormRunner;

public class Topology {
	
	public static TopologyBuilder wireTopology(TopologyConfig topoConfig) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("clickSpout",
				new KafkaSpout(topoConfig.kafkaClickTopic, topoConfig.KafkaClickGroup),
				topoConfig.clickSpoutNum);

		builder.setSpout("impressionSpout",
				new KafkaSpout(topoConfig.kafkaImpressionTopic, topoConfig.KafkaImpressionGroup), 
				topoConfig.impressionSpoutNum);
		builder.setBolt("impressionParseBolt", new ImpressionBolt(), topoConfig.impressionBoltNum)
				.shuffleGrouping("impressionSpout");

		builder.setBolt("clickParseBolt", new ClickBolt(), topoConfig.clickBoltNum)
				.shuffleGrouping("clickSpout");

//		builder.setBolt("intervalCountBolt", new IntervalCountBolt(topoConfig.intervalCountSeconds),
//				topoConfig.intervalCountBoltNum)
//				.fieldsGrouping("impressionParseBolt", new Fields("psid", "infos", "order"))
//				.fieldsGrouping("clickParseBolt", new Fields("psid", "infos", "order"));

//		builder.setBolt("outputBolt", new OutputBolt(), topoConfig.getInt(Constant.OUTPUT_BOLT_NUM))
//				.shuffleGrouping("intervalCountBolt");
		
		return builder;
	}

	public static Config prepareStormConfig(TopologyConfig topoConfig) {

		Config config = new Config();
		config.setDebug(false);

		// kafka configuration
		config.put("kafka.zookeeper.connect", topoConfig.kafkaZookeeper); 
		config.put("kafka.consumer.timeout.ms", topoConfig.kafkaConsumerTimeoutMs);

		// put redis config
		config.put("redis.list", topoConfig.redisConfigList);
		// put alarm config
		config.put(Constant.ALERT_KID, topoConfig.alertKid);
		config.put(Constant.ALERT_PASSWORD, topoConfig.alertPassword);
		config.put(Constant.ALERT_GROUP_NAME, topoConfig.alertGroupName);
		config.put(Constant.ALERT_SERVICE_NAME, topoConfig.alertServiceName);
		config.put(Constant.ALERT_MAIL_TO, topoConfig.alertMailTo);
		config.put(Constant.ALERT_MSG_TO, topoConfig.alertMessageTo);

		config.put(Constant.ALERT_CHECK_INTERVAL_SECONDS, topoConfig.alertCheckInterval);
		config.put(Constant.ALERT_FAILURE_THRESHOLD, topoConfig.alertFailureThreshold);

		// put output kafka config
		config.put(Constant.OUTPUT_KAFKA_BROKER_LIST, topoConfig.get(Constant.OUTPUT_KAFKA_BROKER_LIST));
		config.put(Constant.OUTPUT_KAFKA_TOPIC, topoConfig.get(Constant.OUTPUT_KAFKA_TOPIC));

		// storm config
		config.put("topology.transfer.buffer.size", topoConfig.transferBufferSize);
		config.put("topology.executor.receive.buffer.size", topoConfig.transferBufferSize);
		config.put("topology.executor.send.buffer.size", topoConfig.transferBufferSize);

		config.setNumWorkers(topoConfig.workerNum);
		config.setMaxSpoutPending(topoConfig.maxPendingNum);
		config.setNumAckers(topoConfig.ackNum);
		
		// set metrics consumer
		config.registerMetricsConsumer(OutputMetricsConsumer.class, 1);
		return config;
	}
	
	public static void main(String[] args) {
		if (args.length < 2 || (!args[1].equals("local") && !args[1].equals("remote"))) {
			System.err.println("Usage: Topology configFile (local|remote)");
			System.exit(1);
		}
		boolean runningOnRemote = args[1].equals("remote");

		Properties props = new Properties();
		try {
			props.load(new FileInputStream(args[0]));
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.println("Failed to load Configuration: " + e1.getMessage());
		}
		Enumeration<?> allKeys = props.propertyNames();
		while (allKeys.hasMoreElements()) {
			String key = (String) allKeys.nextElement();
			System.err.println("Load config: " + key + "=" + props.getProperty(key));
		}

		TopologyConfig topoConfig = new TopologyConfig(props); 
		topoConfig.parse();

		Config config = prepareStormConfig(topoConfig);

		config.setNumWorkers(topoConfig.workerNum);
		TopologyBuilder topologyBuilder = wireTopology(topoConfig);
		
		if (runningOnRemote) {
			try {
				StormSubmitter.submitTopology(topoConfig.topologyName, config,
						topologyBuilder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Failed to run topology: " + e.getMessage());
			}
		} else {
			// This setting puts a ceiling on the number of executors that can be spawned for a single component.
			config.setMaxTaskParallelism(2);

			// set log to debug level
			// NOTE: ugly way, storm and the spout print massive debug log!
			// setLoggingLevel(ch.qos.logback.classic.Level.DEBUG);

			try {
				StormRunner.runTopologyLocally(topologyBuilder.createTopology(), "ea_realtime_counter", config, 500);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Failed to run topology: " + e.getMessage());
			}
		}
	}
}
