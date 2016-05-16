package com.bigdatafly.flume.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.lf5.LogLevel;

public class LogEntry {

	String   level; 
	String   logtime;
	String   log;
	
	public LogEntry(){
		
		
	}
	
	public static final String[] logLevels;
	
	public static final int LOG_LEVEL_LEN = 7;
	
	static{
		
		List<String> levelArray = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		List<LogLevel> levels = (List<LogLevel>)LogLevel.getLog4JLevels();
		for(LogLevel level : levels)
			levelArray.add((String.format("[%-5s]", level)));
		logLevels = levelArray.toArray(new String[0]);
	}
	
	
	
	public static void main(String[] args){
		
		List<String> logTypes = new ArrayList<String>();
		List<LogLevel> levels = LogLevel.getLog4JLevels();
		for(LogLevel level : levels)
			logTypes.add((String.format("[%-5s]", level)));
		
		String s = "Size of avail pool [INFO ] [09:09:35] SockIOPool:1578- ++++ Size of avail pool for host (172.16.15.80:11215) = 5";
		int pos = StringUtils.indexOfAny(s,logTypes.toArray(new String[0]) );
		System.out.println(pos);
		
	}
	
	
}
