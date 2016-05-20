/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.List;

/**
 * @author summer
 *
 */
public interface ILog4jParser {

	public List<LogEntry> parse(StringBuffer log);

	public ILog4jParser setPattern(String pattern);
}
