/**
 * 
 */
package com.bigdatafly.flume.log;

/**
 * @author summer
 *
 */
public class LogEvent extends LogEntry{

	/**
	 * 
	 */
	
	public static final String LOG_LEVEL_KEY ="loglevel";
	public static final String LOG_TIME_KEY = "logtime";
	
	String   level; 
	String   logtime;
	String   log;
	int len;
	String ip;
	
	
	public String getLevel() {
		return level;
	}
	public void setLevel(String level) {
		this.level = level;
	}
	public String getLogtime() {
		return logtime;
	}
	public void setLogtime(String logtime) {
		this.logtime = logtime;
	}
	public String getLog() {
		return log;
	}
	public void setLog(String log) {
		this.log = log;
	}
	public int getLen() {
		return len;
	}
	public void setLen(int len) {
		this.len = len;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	@Override
	public String toString() {
		
		return super.toString();
	}
	
	
	

}
