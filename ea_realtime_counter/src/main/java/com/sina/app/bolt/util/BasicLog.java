package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public abstract class BasicLog {
	
	public String[] parts;
	
	// mark
	public boolean isValid = true;

	// common part
	public String time;
	public String customer;
	public String psid;
	public String creative;
	public String lineitem;
	public String group;
	public String platType;
	public String ideaType;
	
	public String order;
	public String channel;
	
	public BasicLog(String log, LogFormat format) {
		String[] segs = StringUtils.splitPreserveAllTokens(log, '\t');
		if (segs == null | segs.length < format.logLength) {
			isValid = false;
			return;
		}
		parts = segs;
		
		time = parts[format.timestampLoc];
		customer = parts[format.customerLoc];
		psid = parts[format.pdpsLoc];
		creative = parts[format.creativeLoc];
		lineitem = parts[format.deliverLoc];
		group = parts[format.groupLoc];
		platType = parts[format.platLoc];
		ideaType = parts[format.ideaTypeLoc];
	}
}
