package com.bigdatafly.flume.log;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.lf5.LogLevel;

import com.bigdatafly.flume.utils.JsonUtils;

public class LogEntry {

	protected String   level; 
	protected String   logtime;
	protected String   log;
	
	public static final String[] logLevels;
	
	public static final int LOG_LEVEL_LEN = 7;
	public static final int LOG_TIME_LEN = 10;
	
	static{
		
		List<String> levelArray = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		List<LogLevel> levels = (List<LogLevel>)LogLevel.getLog4JLevels();
		for(LogLevel level : levels)
			levelArray.add((String.format("[%-5s]", level)));
		logLevels = levelArray.toArray(new String[0]);
	}
	
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



	public static String[] getLoglevels() {
		return logLevels;
	}

	

	@Override
	public String toString() {
		
		return JsonUtils.toJson(this);
	}	
	
}
