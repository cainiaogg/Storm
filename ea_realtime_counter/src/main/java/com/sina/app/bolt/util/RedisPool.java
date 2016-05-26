package com.sina.app.bolt.util;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Wrapper for redis operation.
 * @author xiaocheng1
 *
 */
public class RedisPool {
	private static final Logger LOG = LoggerFactory.getLogger(RedisPool.class);

	Queue<RedisVisitor> queue;

	private class RedisVisitor {
		public final String host;
		public final int port;
		
		Jedis client;
		boolean available;

		public RedisVisitor(String host, int port) {
			this.host = host;
			this.port = port;
			this.client = null;
			available = false;
		}
		
		public void reset() {
			if (client != null && client.isConnected()) {
				try {
					client.quit();
				} catch (Exception e) {
					// pass
				}
			}
			client = new Jedis(host, port);
			available = true;
		}
		
		public void destroy() {
			if (client != null && client.isConnected()) {
				try {
					client.quit();
				} catch (Exception e) {
					// pass
				}
			}
		}
	}
	
	/**
	 * Redis config format: host1:port,host2:port,...
	 * @param redisConfigString
	 */
	public RedisPool(String redisConfigString) {
		queue = new LinkedList<RedisVisitor>();

		String[] nodeList = StringUtils.split(redisConfigString, ",;");
		for (String node: nodeList) {
			String[] parts = StringUtils.split(node, ':');
			if (parts.length != 2)
				continue;

			RedisVisitor visitor = new RedisVisitor(parts[0], Integer.parseInt(parts[1]));
			visitor.reset();
			queue.add(visitor);
		}

		if (queue.size() == 0) {
			throw new IllegalArgumentException("Wrong redis config: " + redisConfigString);
		}
	}

	public int size() {
		return queue.size();
	}
	
	public String getRandomKey() {
		String key = null;
		int maxTryTime = queue.size();
		for (int ix = 0; ix < maxTryTime; ix++) {
			RedisVisitor visitor = queue.poll();
			if (visitor == null) {
				break;
			}
			if (visitor.available && (key = getRandomKey(visitor)) != null) {
				queue.add(visitor);
				return key;
			}
			visitor.reset();
			queue.add(visitor);
		}

		LOG.error("No available redis!");
		return key;
	}
	
	private String getRandomKey(RedisVisitor visitor) {
		try {
			return visitor.client.randomKey();
		} catch (Exception e) {
			LOG.error("Failed to get random key (" + visitor.host + ":" + visitor.port + "): " + e);
		}
		return null;
	}
	
	public boolean store(byte[] listKey, byte[] key, byte[] value, int expireTime) {
		int maxTryTime = queue.size();
		for (int ix = 0; ix < maxTryTime; ix++) {
			RedisVisitor visitor = queue.poll();
			if (visitor == null) {
				break;
			}
			if (visitor.available && store(visitor, listKey, key, value, expireTime)) {
				queue.add(visitor);
				return true;
			}
			visitor.reset();
			queue.add(visitor);
		}

		LOG.error("No available redis!");
		return false;
	}

	/**
	 * Store the key-value, and push the key into key_list.
	 */
	private boolean store(RedisVisitor visitor, byte[] listKey, byte[] key, byte[] value, int expireTime) {
		try {
			visitor.client.set(key, value);
		} catch (Exception e) {
			LOG.error("Failed to store key " + key + " (" + visitor.host + ":" + visitor.port + "): " + e);
			return false;
		}
		try {
			visitor.client.expire(key, expireTime);
		} catch (Exception e) {
			LOG.error("Failed to expire key " + key + " (" + visitor.host + ":" + visitor.port + "): " + e);
		}
		try {
			visitor.client.rpush(listKey, key);
		} catch (Exception e) {
			LOG.error("Failed to push to list, key " + key + " (" + visitor.host + ":" + visitor.port + "): " + e);
			return false;
		}
		return true;
	}

	public void destroy() {
		for (RedisVisitor visitor: queue) {
			visitor.destroy();
		}
	}
}
