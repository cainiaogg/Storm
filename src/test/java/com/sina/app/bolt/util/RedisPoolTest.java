package com.sina.app.bolt.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RedisPoolTest {

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void test() {
		RedisPool pool = new RedisPool("10.210.228.84:9901,10.210.228.84:9902;10.210.228.84:6382");
		assertEquals(pool.size(), 3);
//		System.out.println(pool.getRandomKey());
		pool.destroy();
	}

}
