/**
 * 
 */
package com.bigdatafly.flume.common;

import java.util.List;

/**
 * @author summer
 *
 */
public class DataVo implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2666976175313104003L;

	int dataSize;
	
	List<LogEntry> entries;

	public int getDataSize() {
		return dataSize;
	}

	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}

	public List<LogEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<LogEntry> entries) {
		this.entries = entries;
	}
	
	
	
	
}
