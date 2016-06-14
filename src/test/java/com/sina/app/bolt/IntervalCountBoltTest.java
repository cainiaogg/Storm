/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sina.app.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IntervalCountBoltTest {

	private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
	private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";
	private static final int tickFrequency = 3;

	private Tuple mockNormalTuple(Object... args) {
		Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
		// assemble a tuple for String and Integer
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				when(tuple.getString(i)).thenReturn((String) args[i]);
			} else {
				when(tuple.getInteger(i)).thenReturn((Integer) args[i]);
			}
		}

		return tuple;
	}

	@SuppressWarnings("rawtypes")
	@Test
	/**
	 * Should emit nothing.
	 */
	public void noObjectCounted() {
		// given
		Tuple tickTuple = MockTupleHelpers.mockTickTuple();

		IntervalCountBolt bolt = new IntervalCountBolt(tickFrequency);
		Map conf = mock(Map.class);
		when(conf.get("interval.count.seconds")).thenReturn("3");
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		// when
		bolt.execute(tickTuple);

		// then
		// TODO: should we ack tick tuple??
		// verifyZeroInteractions(collector);
		verify(collector).ack(any(Tuple.class));
		// should not emit
		verify(collector, never()).emit(any(Values.class));
	}

	@SuppressWarnings("rawtypes")
	@Test
	/**
	 * Should emit something.
	 */
	public void hasObjectCounted() {
		// given
		Tuple tickTuple = MockTupleHelpers.mockTickTuple();

		IntervalCountBolt bolt = new IntervalCountBolt(tickFrequency);
		Map conf = mock(Map.class);
		when(conf.get("interval.count.seconds")).thenReturn("3");
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		// when
		bolt.execute(mockNormalTuple("plat1", "stat", "psid1", "info", "1", 1));
		bolt.execute(mockNormalTuple("plat1", "stat", "psid2", "info", "1", 1));
		bolt.execute(mockNormalTuple("plat2", "stat", "psid3", "info", "1", 1));
		bolt.execute(tickTuple);

		// then should emit two values
		verify(collector, times(2)).emit(any(Values.class));
	}

	@Test
	public void shouldDeclareOutputFields() {
		// given
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		IntervalCountBolt bolt = new IntervalCountBolt(tickFrequency);

		// when
		bolt.declareOutputFields(declarer);

		// then
		verify(declarer).declare(any(Fields.class));
	}

	@Test
	public void checkTickTupleConfiguration() {
		// given
		IntervalCountBolt bolt = new IntervalCountBolt(tickFrequency);

		// when
		Map<String, Object> componentConfig = bolt.getComponentConfiguration();

		// then
		assertTrue(componentConfig.containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
		Integer emitFrequencyInSeconds = (Integer) componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
		assertTrue(emitFrequencyInSeconds > 0);
	}
}
