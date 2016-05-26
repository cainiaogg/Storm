package com.sina.app.bolt.util;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Wrapper for kafka writing operation.
 * @author xiaocheng1
 *
 */
public class KafkaClient {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
	
	private final kafka.javaapi.producer.Producer<String, byte[]> producer;
	private final String topic;
	private final Properties props = new Properties();

	public KafkaClient(String brokerList, String topic) {
		props.put("metadata.broker.list", brokerList);

		// we have string key and byte[] message
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");

		// require an acknowledgement for broker
		props.put("request.required.acks", "1");

		producer = new kafka.javaapi.producer.Producer<String, byte[]>(
				new ProducerConfig(props));
		this.topic = topic;
	}

	public boolean send(String key, byte[] message) {
		try {
			producer.send(new KeyedMessage<String, byte[]>(topic, key, message));
			return true;
		} catch (kafka.common.MessageSizeTooLargeException e) {
			LOG.error("Message too large: len=" + message.length);
		} catch (kafka.common.FailedToSendMessageException e) {
			LOG.error("Failed to send: " + e.getMessage());
		} catch (Exception e) {
			LOG.error("Failed to send: " + e.getMessage());
		}
		return false;
	}

	public void reconnect() {
	}
	
	public void destroy() {
		producer.close();
	}
}
