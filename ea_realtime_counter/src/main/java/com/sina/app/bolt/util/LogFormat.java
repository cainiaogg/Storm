package com.sina.app.bolt.util;


/**
 * Configuration of log format.
 * @author xiaocheng1
 *
 */
public class LogFormat {
	
	public int platLoc = -1;
	public int ideaTypeLoc = -1;
	public int pdpsLoc = -1;
	public int customerLoc = -1;
	public int groupLoc = -1;
	public int deliverLoc = -1;
	public int creativeLoc = -1;
	public int timestampLoc = -1;
	public int logLength = -1;
	
	// NOTE: only available for click log
	public int orderInfoLoc = -1;
	// index of the field identifying whether a click log is cheating
	public int cheatLoc = -1;
	
	// NOTE: only available for impress log
	public int algodataLoc = -1;
}
