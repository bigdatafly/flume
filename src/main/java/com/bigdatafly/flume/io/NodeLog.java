/**
 * 
 */
package com.bigdatafly.flume.io;

import java.io.Serializable;

import com.google.gson.Gson;

/**
 * @author summer
 *
 */
public class NodeLog implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8519142131643727217L;

	String host ;
	long   flow = 0;
	long   time = 0;
	
	public NodeLog(){
		
		host = "";
		time = System.currentTimeMillis();
	}
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public long getFlow() {
		return flow;
	}
	public void setFlow(long flow) {
		this.flow = flow;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	
	public String toJson(){
		String s = "";
		Gson gson = new Gson();
		s = gson.toJson(this);
		return s;
		
	}
	
	
	
	@Override
	public String toString() {
		return "NodeLog [host=" + host + ", flow=" + flow + ", time=" + time + "]";
	}

	public static void main(String[] args){
		
		NodeLog nodelog = new NodeLog();
		nodelog.setHost("Unknown Host");
		nodelog.setFlow(1000);
		nodelog.setTime(System.currentTimeMillis());
		
		System.out.println(nodelog.toJson());;
	}
	
}
