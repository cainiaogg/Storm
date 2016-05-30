package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ClickLog {
	public int clickLength = 17;
	public boolean isValid = true;
	public String uuid;
	public String logclkVal;
	public ClickLog(String log) {
		String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
		if (segs.length < clickLength) {
			isValid = false;
			return;
		}
		uuid = segs[3];
		logclkVal = log;
	}
}
