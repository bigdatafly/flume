/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.lf5.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author summer
 *
               如果使用pattern布局就要指定的打印信息的具体格式ConversionPattern，具体参数： 
	   %m 输出代码中指定的消息 
	　%p 输出优先级，即DEBUG，INFO，WARN，ERROR，FATAL 
	　%r 输出自应用启动到输出该log信息耗费的毫秒数 
	　%c 输出所属的类目，通常就是所在类的全名  
	　%t 输出产生该日志事件的线程名 
	　%n 输出一个回车换行符，Windows平台为"rn”，Unix平台为"n” 
	　%d 输出日志时间点的日期或时间，默认格式为ISO8601，也可以在其后指定格式，比如：%d{yyyy MM dd HH:mm:ss,SSS}，输出类似：2002年10月18日 22：10：28，921 　 %l 输出日志事件的发生位置，包括类目名、发生的线程，以及在代码中的行数。 
	   %x: 输出和当前线程相关联的NDC(嵌套诊断环境),尤其用到像java servlets这样的多客户多线程的应用中。 
	   %%: 输出一个”%”字符 
	   %F: 输出日志消息产生时所在的文件名称 
	   %M: 输出执行方法 
	   %L: 输出代码中的行号 
	   可以在%与模式字符之间加上修饰符来控制其最小宽度、最大宽度、和文本的对齐方式。如： 
	   1) c：指定输出category的名称，最小的宽度是20，如果category的名称小于20的话，默认的情况下右对齐。 
	   2)%-20c:指定输出category的名称，最小的宽度是20，如果category的名称小于20的话，”-”号指定左对齐。 
	   3)%.30c:指定输出category的名称，最大的宽度是30，如果category的名称大于30的话，就会将左边多出的字符截掉，但小于30的话也不会有空格。 
	   4) .30c:如果category的名称小于20就补空格，并且右对齐，如果其名称长于30字符，就从左边交远销出的字符截掉。 
	   [APPName]是log信息的开头，可以为任意字符，一般为项目简称。 
 * 
 */
public class DefaultLog4jParser extends AbstractLog4jParser  implements ILog4jParser{

	private static final Logger logger = LoggerFactory
			.getLogger(DefaultLog4jParser.class);
	private static final String DEFAULT_PATTERN = "%d{yyyy-MM-dd HH:mm:ss,SSS} %p %c - %m%n";
	
	public static final String[] logLevels;
	
	public static final int LOG_LEVEL_LEN = 5;
	public static final int LOG_TIME_LEN = 23;
	
	static{
		
		List<String> levelArray = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		List<LogLevel> levels = (List<LogLevel>)LogLevel.getLog4JLevels();
		for(LogLevel level : levels)
			levelArray.add((String.format("%-5s", level)));
		logLevels = levelArray.toArray(new String[0]);
	}
	
	private int logLevelLen;
	private int dateTimeLen;
	
	public DefaultLog4jParser(){
		
		this.pattern = DEFAULT_PATTERN;
		this.logLevelLen = 5;
		this.dateTimeLen = "yyyy-MM-dd HH:mm:ss,SSS".length() + 1;
	}
	
	
	public List<LogEntry> parse(StringBuffer sb){
		
		List<LogEntry> logEntries =new ArrayList<LogEntry>();
		
		int beginIndex = 0;
		int endIndex = 0;
		
		while(true){
			  
				String tempStr = sb.toString();
				int len = tempStr.length();
				
				if(len > logLevelLen){
					beginIndex = StringUtils.indexOfAny(tempStr, logLevels);
					
					if(beginIndex <0){
						sb.delete(0, len);
						break;
					}
					else{
						String temp =sb.substring(LOG_LEVEL_LEN+beginIndex);
						endIndex = StringUtils.indexOfAny(temp, logLevels);
						if(endIndex <0){
							break;
						}else{
							
							int startPos = beginIndex-dateTimeLen>=0 ? beginIndex-dateTimeLen : 0;
							
							String strLogEntry = StringUtils.mid(tempStr, startPos, LOG_LEVEL_LEN+endIndex);
							if(logger.isDebugEnabled())
								logger.debug("{"+strLogEntry+"}");
							LogEntry logEntry = convert(strLogEntry);
							if(logEntry!=null)
								logEntries.add(logEntry);
							sb.delete(0, beginIndex + LOG_LEVEL_LEN+endIndex - dateTimeLen);
							
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
			pos = StringUtils.indexOfAny(log, logLevels);
			if(pos == -1)
				return null;
			
			String level = StringUtils.substring(log, pos, pos + LOG_LEVEL_LEN);
			String time = StringUtils.substring(log,0,LOG_TIME_LEN);
			
			String tempStr = StringUtils.substring(log,pos + LOG_LEVEL_LEN+LOG_SPACE_DELIMITER.length());
			pos = StringUtils.indexOf(tempStr, LOG_SPACE_DELIMITER);
			if(pos == -1)
				return null;
			
			if(StringUtils.length(time) ==0 || StringUtils.length(time) < LOG_TIME_LEN)
				return null;
			LogEntry entry = new LogEntry();
			entry.setLevel(level);
			entry.setLogtime(time);
			entry.setLog(tempStr);
			return entry;
		
	}

}
