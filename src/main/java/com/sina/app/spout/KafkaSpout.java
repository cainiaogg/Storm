/**
 * Copyright 2013 Netherlands Forensic Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sina.app.spout;

import static com.sina.app.spout.util.ConfigUtils.CONFIG_FAIL_HANDLER;
import static com.sina.app.spout.util.ConfigUtils.DEFAULT_FAIL_HANDLER;
import static com.sina.app.spout.util.ConfigUtils.createFailHandlerFromString;
import static com.sina.app.spout.util.ConfigUtils.createKafkaConfig;
import static com.sina.app.spout.util.ConfigUtils.getMaxBufSize;
import static com.sina.app.spout.util.ConfigUtils.getTopic;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sina.app.bolt.util.ClickLog;
import com.sina.app.bolt.util.TimeSign;
import org.apache.commons.exec.util.StringUtils;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.sina.app.spout.fail.FailHandler;
import com.sina.app.spout.util.ConfigUtils;
import com.sina.app.spout.util.KafkaMessageId;

public class KafkaSpout implements IRichSpout {
    private static final long serialVersionUID = -1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    protected final SortedMap<KafkaMessageId, byte[]> _inProgress = new TreeMap<KafkaMessageId, byte[]>();
    protected final Queue<KafkaMessageId> _queue = new LinkedList<KafkaMessageId>();
    
    protected String _topic;
    protected String _consumer_group_id;
    
    protected int _bufSize;
    protected FailHandler _failHandler;
    protected ConsumerIterator<byte[], byte[]> _iterator;
    protected transient SpoutOutputCollector _collector;
    protected transient ConsumerConnector _consumer;

    public KafkaSpout(String topicName, String consumer_group) {
    	this._topic = topicName;
    	this._consumer_group_id = consumer_group;
    }

    protected void createFailHandler(final String failHandler) {
        if (failHandler == null) {
            _failHandler = DEFAULT_FAIL_HANDLER;
        }
        else {
            _failHandler = createFailHandlerFromString(failHandler);
        }
    }

    protected void createConsumer(final Map<String, Object> config) {
        final Properties consumerConfig = createKafkaConfig(config);
        
        consumerConfig.put("group.id", this._consumer_group_id);

        LOG.info("connecting kafka client to zookeeper at {} as client group {}",
            consumerConfig.getProperty("zookeeper.connect"),
            consumerConfig.getProperty("group.id"));
        _consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
    }

    protected boolean fillBuffer() {
        if (!_inProgress.isEmpty() || !_queue.isEmpty()) {
            throw new IllegalStateException("cannot fill buffer when buffer or pending messages are non-empty");
        }

        if (_iterator == null) {
            final Map<String, List<KafkaStream<byte[], byte[]>>> streams = _consumer.createMessageStreams(Collections.singletonMap(_topic, 1));
            _iterator = streams.get(_topic).get(0).iterator();
        }

        try {
            int size = 0;
            while (size < _bufSize && _iterator.hasNext()) {
                final MessageAndMetadata<byte[], byte[]> message = _iterator.next();
                final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
                _inProgress.put(id, message.message());
                size++;
            }
        }
        catch (final ConsumerTimeoutException e) {
        }

        if (_inProgress.size() > 0) {
            _queue.addAll(_inProgress.keySet());
            LOG.debug("buffer now has {} messages to be emitted", _queue.size());
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bytes"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
        _collector = collector;
        _bufSize = getMaxBufSize((Map<String, Object>) config);
        createFailHandler((String) config.get(CONFIG_FAIL_HANDLER));
        createConsumer((Map<String, Object>) config);
        _failHandler.open(config, topology, collector);

        LOG.info("kafka spout opened, reading from topic {}, using failure policy {}", _topic, _failHandler.getIdentifier());
    }

    @Override
    public void close() {
        _collector = null;
        _iterator = null;
        if (_consumer != null) {
            try {
                _consumer.shutdown();
            }
            finally {
                _consumer = null;
            }
        }

        _failHandler.close();
    }
    @Override
    public void activate() {
        _failHandler.activate();
    }

    @Override
    public void deactivate() {
        _failHandler.deactivate();
    }

    @Override
    public void nextTuple() {
        if (!_queue.isEmpty() || (_inProgress.isEmpty() && fillBuffer())) {
            final KafkaMessageId nextId = _queue.poll();
            if (nextId != null) {
                final byte[] message = _inProgress.get(nextId);
                if (message == null) {
                    throw new IllegalStateException("no pending message for next id " + nextId);
                }
                _collector.emit(new Values((Object) message), nextId);
                LOG.debug("emitted kafka message id {} ({} bytes payload)", nextId, message.length);
            }
        }
    }

    @Override
    public void ack(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId id = (KafkaMessageId) o;
            _inProgress.remove(id);
            LOG.debug("kafka message {} acknowledged", id);
            if (_inProgress.isEmpty()) {
                LOG.debug("all pending messages acknowledged, committing client offsets");
                _consumer.commitOffsets();
            }
            _failHandler.ack(id);
        }
    }

    @Override
    public void fail(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId id = (KafkaMessageId) o;
            if (_failHandler.shouldReplay(id)) {
                LOG.debug("kafka message id {} failed in topology, adding to buffer again", id);
                _queue.add(id);
            }
            else {
                LOG.debug("kafka message id {} failed in topology, delegating failure to policy", id);
                _failHandler.fail(id, _inProgress.remove(id));
            }
        }
    }
}
