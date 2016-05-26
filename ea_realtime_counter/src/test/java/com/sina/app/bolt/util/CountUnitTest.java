package com.sina.app.bolt.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;


public class CountUnitTest {
	protected CountUnit target;
	
	@Before
	public void setUp() {
		target = new CountUnit("pc_image", "psid_item");
	}

	@Test
	public void increment() {
		assertEquals(target.getPv("psid1", "item1", "1"), 0);
		assertEquals(target.getClick("psid1", "item1", "1"), 0);
		target.increment("psid1", "item1", "1", 0);
		target.increment("psid1", "item1", "1", 0);
		target.increment("psid1", "item1", "1", 1);
		target.increment("psid1", "item1", "1", 0);
		assertEquals(target.getPv("psid1", "item1", "1"), 3);
		assertEquals(target.getClick("psid1", "item1", "1"), 1);
		assertEquals(target.getClick("psid22", "item1", "1"), 0);
	}

	@Test
	public void outputTableMessage() {
		System.out.println("TableString:");
		System.out.println(target.toTableString());
		System.out.println("SerializedTable:");
		System.out.println(target.toSerializedTable());

		target.increment("psid1", "item1", "1", 0);
		target.increment("psid1", "item1", "1", 0);
		target.increment("psid1", "item1", "1", 1);
		target.increment("psid1", "item1", "1", 0);
		
		target.increment("psid1", "item2", "1", 0);
		target.increment("psid1", "item2", "1", 0);
		target.increment("psid1", "item2", "1", 1);

		System.out.println("TableString:");
		System.out.println(target.toTableString());
		System.out.println("SerializedTable:");
		System.out.println(target.toSerializedTable());
	}

}
