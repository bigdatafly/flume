/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

/**
 * @author summer
 *
 */
public class LogEvent extends LogEntry implements Event{

	/**
	 * 
	 */
	
	public static final String LOG_LEVEL_KEY ="loglevel";
	public static final String LOG_TIME_KEY = "logtime";
	
	Map<String, String> headers;
	byte[] body;
	
	public LogEvent(){
		
	}
	

	public Map<String, String> getHeaders() {
		
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		
		if(this.headers == null)
			this.headers = new HashMap<String,String>();
		if(headers !=null)
			this.headers = new HashMap<String,String>(headers);
		
	}

	public byte[] getBody() {
		
		return body;
	}

	public void setBody(byte[] body) {
		
		this.body = body;
		
	}

}
