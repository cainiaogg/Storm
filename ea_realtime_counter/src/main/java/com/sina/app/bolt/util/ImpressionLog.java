package com.sina.app.bolt.util;

import org.apache.commons.lang.StringUtils;

public class ImpressionLog extends BasicLog {
	private static final LogFormat format = new LogFormat();
	// set impress log structure
	static {
		format.timestampLoc = 0;
		//format.iploc= 1;
		format.pdpsLoc = 4;
		format.deliverLoc = 6;
		format.creativeLoc = 7;
		format.customerLoc = 10;
		format.groupLoc = 12;
		format.algodataLoc = 20;
		format.platLoc = 21;
		format.ideaTypeLoc = 22;
		format.logLength = 25;
	};

	public String algoExtField;

	public ImpressionLog(String log) {
		super(log, format);
		if (this.isValid) {
			parse();
		}
	}

	protected void parse() {
		// get ext field
		algoExtField = parts[format.algodataLoc];
		
		// parse
		String[] words = StringUtils.splitPreserveAllTokens(algoExtField, '|');
		for (int i = 0; i < words.length; ++i) {
			String[] items = StringUtils.splitPreserveAllTokens(words[i], ':');
			if (items.length != 2)
				continue;
			if (items[0].startsWith("or")) {
				order = items[1];
			} else if (items[0].equals("chan") 
					&& !items[1].equals("null")
					&& items[1].length() > 0) {
				channel = items[1];
			}
		}
	}
}
