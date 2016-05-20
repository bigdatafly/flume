/**
 * 
 */
package com.bigdatafly.flume.log;

/**
 * @author summer
 *
 */
public abstract class AbstractLog4jParser implements ILog4jParser{

	protected String  pattern;

	public ILog4jParser setPattern(String pattern) {
		
		this.pattern = pattern;
		
		return this;
	}
	
	public static interface Serializable{
		
		public LogEntry serializable(String log);
	}
	
}
