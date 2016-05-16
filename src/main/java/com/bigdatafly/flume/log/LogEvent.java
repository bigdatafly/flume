/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.Map;

import org.apache.flume.Event;

import com.bigdatafly.flume.common.LogEntry;

/**
 * @author summer
 *
 */
public class LogEvent extends LogEntry implements Event{

	/**
	 * 
	 */

	public Map<String, String> getHeaders() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setHeaders(Map<String, String> headers) {
		// TODO Auto-generated method stub
		
	}

	public byte[] getBody() {
		
		return null;
	}

	public void setBody(byte[] body) {
		// TODO Auto-generated method stub
		
	}

}
