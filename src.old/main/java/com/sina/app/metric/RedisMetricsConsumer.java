package com.sina.app.metric;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.sina.app.util.Constant;

public class RedisMetricsConsumer implements IMetricsConsumer {
	public static final Logger LOG = LoggerFactory.getLogger(RedisMetricsConsumer.class);
	public static final String REDIS_COMPONENT_ID = "redisBolt";
	public static final String REDIS_FAILURE_METRIC = "redisFailureCounter";
	private int alarmThreshold;
	private AlertMailBox mailBox;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		alarmThreshold = Integer.parseInt(String.valueOf(stormConf.get(Constant.ALERT_FAILURE_THRESHOLD)));
		mailBox = new AlertMailBox(stormConf);
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		long errorCount = 0;
		if (taskInfo.srcComponentId.equals(REDIS_COMPONENT_ID)) {
			for (DataPoint point: dataPoints) {
				if (point.name.equals(REDIS_FAILURE_METRIC)) {
					errorCount += (Long) point.value;
				}
			}
		}
		if (errorCount > alarmThreshold) {
			LOG.error("Redis failure count: " + errorCount + ", send alarm email.");
			mailBox.sendAlert(taskInfo.srcWorkerHost + "_" + taskInfo.srcWorkerPort, errorCount);
		} else if (errorCount > 0) {
			LOG.error("Redis failure count: " + errorCount);
		}
	}

	@Override
	public void cleanup() {
	}

}
