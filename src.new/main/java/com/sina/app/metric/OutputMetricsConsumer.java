package com.sina.app.metric;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.sina.app.util.Constant;

/**
 * Metrics consumer to watch failure of output.
 * @author xiaocheng1
 *
 */
public class OutputMetricsConsumer implements IMetricsConsumer {
	public static final Logger LOG = LoggerFactory.getLogger(OutputMetricsConsumer.class);
	public static final String COMPONENT_ID = "outputBolt";
	public static final String FAILURE_METRIC = "outputFailureCounter";
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
		if (taskInfo.srcComponentId.equals(COMPONENT_ID)) {
			for (DataPoint point: dataPoints) {
				if (point.name.equals(FAILURE_METRIC)) {
					errorCount += (Long) point.value;
				}
			}
		}
		if (errorCount > alarmThreshold) {
			LOG.error("Output failure count: " + errorCount + ", send alarm email.");
			mailBox.sendAlert(taskInfo.srcWorkerHost + "_" + taskInfo.srcWorkerPort, errorCount);
		} else if (errorCount > 0) {
			LOG.error("Output failure count: " + errorCount);
		}
	}

	@Override
	public void cleanup() {
	}

}
