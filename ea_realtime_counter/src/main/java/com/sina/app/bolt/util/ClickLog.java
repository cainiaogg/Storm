package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ClickLog extends BasicLog {
	private static final LogFormat format = new LogFormat();
	// set click log structure
	static {
		format.timestampLoc = 0;
		//format.ipLoc = 1;
		format.deliverLoc = 4;
		format.customerLoc = 5;
		format.groupLoc = 6;
		format.cheatLoc = 9;
		format.orderInfoLoc = 10;
		format.pdpsLoc = 11;
		format.creativeLoc = 12;
		format.platLoc = 13;
		format.ideaTypeLoc = 14;
		format.logLength = 15;
	}
	
	public String extField;
	public String cheatField;
	
	public ClickLog(String log) {
		super(log, format);
		if (this.isValid) {
			parse();
		}
	}
	
	/**
	 * Return true if current click log is a cheating click.
	 * @return
	 */
	public boolean isCheatClick() {
		return (! cheatField.equals("0"));
	}
	
	protected void parse() {
		extField = parts[format.orderInfoLoc];
		
		// parse order and channel
		String[] words = StringUtils.splitPreserveAllTokens(extField, '|');
		if (words.length >= 3) {
			order = words[2];
		}
		if (words.length >= 7 && !words[6].equals("null") && words[6].length() > 0) {
			channel = words[6];
		}
		
		// parse cheat field
		cheatField = parts[format.cheatLoc];
	}
}
