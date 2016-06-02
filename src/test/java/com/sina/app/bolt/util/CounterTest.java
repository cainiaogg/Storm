package com.sina.app.bolt.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class CounterTest {

	protected Counter counter;
	
	@Before
	public void setUp() throws Exception {
		counter = new Counter();
	}

	@Test
	public void testIncrement() {	
		for (int i = 0; i < 30; i++) {
			counter.increment("pc_image", "psid_item", "psid1", "item1", "1", 0);
		}
		for (int i = 0; i < 7; i++) {
			counter.increment("pc_image", "psid_item", "psid1", "item1", "1", 1);
		}
		counter.increment("pc_image", "psid_item", "psid2", "item1", "1", 0);
		counter.increment("pc_image", "psid_item", "psid1", "item2", "1", 1);
		
		counter.print();
	}

}
