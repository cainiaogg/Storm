package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ClickLog extends FormatLog{
	public int clickLength = 17;
	public boolean isValid = true;
	public String uuid;
	public String logclkVal;
	public static final String tableCloumn = "logclk"; //lie
	public ClickLog(String log) {
		String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
		if (segs.length < clickLength) {
			isValid = false;
			return;
		}
		uuid = segs[3];
		int valHash = getHash(uuid);
		if(valHash >= sampleCnt) {
			isValid = false;
			return ;
		}
		logclkVal = log;
	}
}
