package com.bigdatafly.flume.io;

import java.io.Serializable;


public class LogInfo implements Serializable {
	private static final long serialVersionUID = -7058987829722280161L;
	private String hostName;
	private String body;
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	
}
