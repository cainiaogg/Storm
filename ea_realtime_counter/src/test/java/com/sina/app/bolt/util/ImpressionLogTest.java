package com.sina.app.bolt.util;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class ImpressionLogTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testImpressionLog() throws IOException {
		String raw = IOUtils.toString(getClass().getResourceAsStream("/impression.log"), "UTF-8");
		
		ImpressionLog log = new ImpressionLog(raw);
		System.out.println("isValid=" + log.isValid);
		System.out.println("psid=" + log.psid);
		System.out.println("channel=" + log.channel);
		System.out.println("creative=" + log.creative);
		System.out.println("customer=" + log.customer);
		System.out.println("algoExtField=" + log.algoExtField);
		System.out.println("group=" + log.group);
		System.out.println("platType=" + log.platType);
		System.out.println("ideaType=" + log.ideaType);
		System.out.println("lineitem=" + log.lineitem);
		System.out.println("order=" + log.order);
		System.out.println("time=" + log.time);
	}

}
