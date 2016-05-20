/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author summer
 *
 */
public class SimpleLog4jParser extends AbstractLog4jParser  implements ILog4jParser {

	private static final Logger logger = LoggerFactory
			.getLogger(SimpleLog4jParser.class);
	
	
	public List<LogEntry> parse(StringBuffer sb){
		
		List<LogEntry> logEntries =new ArrayList<LogEntry>();
		
		int beginIndex = 0;
		int endIndex = 0;
		
		while(true){
			  
				String tempStr = sb.toString();
				int len = tempStr.length();
				
				if(len > LogEntry.LOG_LEVEL_LEN){
					beginIndex = StringUtils.indexOfAny(tempStr, LogEntry.logLevels);
					
					if(beginIndex <0){
						sb.delete(0, len);
						break;
					}
					else{
						String temp =sb.substring(LogEntry.LOG_LEVEL_LEN+beginIndex);
						endIndex = StringUtils.indexOfAny(temp, LogEntry.logLevels);
						if(endIndex <0){
							break;
						}else{
							
							String strLogEntry = StringUtils.mid(tempStr, beginIndex, LogEntry.LOG_LEVEL_LEN+endIndex);
							if(logger.isDebugEnabled())
								logger.debug("{"+strLogEntry+"}");
							LogEntry logEntry = convert(strLogEntry);
							if(logEntry!=null)
								logEntries.add(logEntry);
							sb.delete(0, beginIndex + LogEntry.LOG_LEVEL_LEN+endIndex);
							
						}
					}
					
				}else{
					break;
				}
				
		}
		/*
		 if(rSize > tempString.length()){ 
		      strBuf.append(tempString.substring(fromIndex, tempString.length())); 
		    //  size+=strBuf.toString().getBytes().length;
		      }else{ 
		      strBuf.append(tempString.substring(fromIndex, rSize)); 
		    //  size+=strBuf.toString().getBytes().length;
	    } 
	    */
		return logEntries;
	}
	
	static final String LOG_SPACE_DELIMITER = " ";
	
	static LogEntry convert(final String log){

			if(StringUtils.isEmpty(log))
				return null;
			int pos = 0;
			pos = StringUtils.indexOfAny(log, LogEntry.logLevels);
			if(pos == -1)
				return null;
			
			String level = StringUtils.substring(log, pos, pos + LogEntry.LOG_LEVEL_LEN);
			String tempStr = StringUtils.substring(log,pos + LogEntry.LOG_LEVEL_LEN+LOG_SPACE_DELIMITER.length());
			pos = StringUtils.indexOf(tempStr, LOG_SPACE_DELIMITER);
			if(pos == -1)
				return null;
			String time = StringUtils.substring(tempStr,0,pos);
			if(StringUtils.length(time) ==0 || StringUtils.length(time) < LogEntry.LOG_TIME_LEN)
				return null;
			LogEntry entry = new LogEntry();
			entry.setLevel(level);
			entry.setLogtime(time);
			entry.setLog(log);
			return entry;
		
	}

}
