package com.sina.app.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.app.util.Constant;
import com.sina.app.util.SinaWatchClient;

public class AlertMailBox {
	private static final Logger LOG = LoggerFactory.getLogger(AlertMailBox.class);
	private static final String ALERT_SUBJECT = "adalgo-ea-rts-storm error";
	private List<NameValuePair> parameters;
	private SinaWatchClient client;

	private String checkInterval;

	@SuppressWarnings("rawtypes")
	public AlertMailBox(Map conf) {
		checkInterval = (String) conf.get(Constant.ALERT_CHECK_INTERVAL_SECONDS);

		client = new SinaWatchClient();
		client.bind_auth(
				(String) conf.get(Constant.ALERT_KID),
				(String) conf.get(Constant.ALERT_PASSWORD));

		parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair(
				"sv", (String) conf.get(Constant.ALERT_GROUP_NAME)));
		parameters.add(new BasicNameValuePair(
				"service", (String) conf.get(Constant.ALERT_SERVICE_NAME)));
		parameters.add(new BasicNameValuePair("subject", ALERT_SUBJECT));	
		parameters.add(new BasicNameValuePair(
				"mailto", (String) conf.get(Constant.ALERT_MAIL_TO)));
		parameters.add(new BasicNameValuePair(
				"msgto", (String) conf.get(Constant.ALERT_MSG_TO)));
	}

	public boolean sendAlert(String src, long errorCount) {
		List<NameValuePair> currentParams = new ArrayList<NameValuePair>(parameters);
		currentParams.add(new BasicNameValuePair("object", src));
		currentParams.add(new BasicNameValuePair("content", 
				"Too many failures, check the redis!"
				+ "\n\rError count: " + errorCount 
				+ "\n\rWatch time(s): "	+ checkInterval));
		try {
			String response = client.post("/v1/alert/send", currentParams);
			LOG.info("Alert response: " + response);
		} catch (Exception e) {
			LOG.error("Alert failed: " + e);
			return false;
		}
		return true;
	}
}
