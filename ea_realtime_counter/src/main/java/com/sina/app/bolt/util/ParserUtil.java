package com.sina.app.bolt.util;

import com.google.common.collect.ImmutableMap;

public class ParserUtil {

	public static final String STAT_TYPE_PSID = "psid";
	public static final String STAT_TYPE_PSID_LINEITEM = "psid_item";
	public static final String STAT_TYPE_PSID_IDEA = "psid_idea";
	public static final String STAT_TYPE_PSID_GROUP = "psid_group";
	public static final String STAT_TYPE_PSID_CUST = "psid_cust";

	// max length of valid platType/ideaType
	private static final int TYPE_MAX_LENGTH = 10;
	
	private static final ImmutableMap<String, String> IDEA_TYPE_MAPPING = ImmutableMap.<String, String>builder()
			.put("text", "txt")
			.put("flash", "img")
			.put("adbox", "img")
			.put("image", "img")
			.build();

	
	/**
	 * Get adtype. If platType or ideaType is not valid, return null.
	 * @param platType
	 * @param ideaType
	 * @return
	 */
	public static String formatAdtype(String platType, String ideaType) {
		if (!isValidType(platType) || !isValidType(ideaType)) {
			return null;
		}
		
		String val = IDEA_TYPE_MAPPING.get(ideaType);
		if (val != null) {
			return platType + "_" + val;
		}
		// unknown case
		return platType + "_" + ideaType;
	}
	
	/**
	 * check the type, length in [0, TYPE_MAX_LENGTH], all chars in 'a'-'z', 'A'-'Z', '_'
	 * @param type
	 * @return
	 */
	public static boolean isValidType(String type){
		if (type == null || type.length() == 0 || type.length() > TYPE_MAX_LENGTH)
			return false;
		for (int ix = 0; ix < type.length(); ix++) {
			char ch = type.charAt(ix);
			if (!(ch >= 'a' && ch <='z') && !(ch >='A' && ch <='Z') && (ch != '_')) {
				return false;
			}
		}
		return true;
	}
}
