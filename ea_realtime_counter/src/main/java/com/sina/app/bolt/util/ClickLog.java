package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ClickLog {
	public String[] strLog = {
			"timestamp",
			"ip",
			"cookie",
			"uuid",
			"lineitemId",
			"accountId",
			"groupId",
			"maxprice",
			"costType",
			"flag",
			"index",
			"unitId",
			"adId",
			"platform",
			"adType",
			"useRetargeting",
			"clickType",
	};
	public int clickLength = 17;
	public boolean isValid = true;
	public String[] logValue;
	public String uuid;

	public ClickLog(String log) {
		String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
		if (segs.length != clickLength) {
			isValid = false;
			return;
		}
		uuid = segs[3];
		for (int i = 0; i < 17; i++) {
			logValue[i] = segs[i];
		}

	}
}
