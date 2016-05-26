package com.sina.app.bolt.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ParserUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testFormatAdtype() {
		assertEquals(ParserUtil.formatAdtype("PC", "text"), "PC_txt");
		assertEquals(ParserUtil.formatAdtype("PC", "image"), "PC_img");
		assertEquals(ParserUtil.formatAdtype("PC", "flash"), "PC_img");
		assertEquals(ParserUtil.formatAdtype("PC", "adbox"), "PC_img");
		assertEquals(ParserUtil.formatAdtype("PC", "xxl"), "PC_xxl");
	}

	@Test
	public void testIsValidType() {
		assertTrue(ParserUtil.isValidType("wap"));
		assertTrue(ParserUtil.isValidType("WAP"));
		assertTrue(ParserUtil.isValidType("image"));
		assertTrue(ParserUtil.isValidType("MOB_UNION"));
		assertTrue(ParserUtil.isValidType("xxl"));
		assertTrue(ParserUtil.isValidType("image_"));

		assertFalse(ParserUtil.isValidType("wap-image"));
		assertFalse(ParserUtil.isValidType("wap1"));
		assertFalse(ParserUtil.isValidType("1234"));
		assertFalse(ParserUtil.isValidType("12345_"));
		assertFalse(ParserUtil.isValidType("&abc"));
		assertFalse(ParserUtil.isValidType("wap."));
		assertFalse(ParserUtil.isValidType("xxl.image"));
		assertFalse(ParserUtil.isValidType("abcdeabcdea"));
	}

}
