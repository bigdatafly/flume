package com.bigdatafly.flume.utils;


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;


public class OSUtils {

	public static void main(String [] args){
		InetAddress netAddress = getInetAddress();
		System.out.println("host ip:" + getHostIp(netAddress));
		System.out.println("host ip:" + getLinuxHostIp());
		System.out.println("host name:" + getHostName(netAddress));
		Properties properties = System.getProperties();
		Set<String> set = properties.stringPropertyNames(); //获取java虚拟机和系统的信息。
		for(String name : set){
			System.out.println(name + ":" + properties.getProperty(name));
		}
	}

	public static InetAddress getInetAddress(){

	    try{
	    	return InetAddress.getLocalHost();
	    }catch(UnknownHostException e){
			System.out.println("unknown host!");
		}
	    return null;

	}

	public static String getHostIp(InetAddress netAddress){
		if(null == netAddress){
			return null;
		}
		
		if(isLinux())
			return getLinuxHostIp();
		
		String ip = netAddress.getHostAddress(); //get the ip address
		return ip;
	}

	public static String getLinuxHostIp(){
		
		String ip = "0.0.0.0";
		
		try{
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			InetAddress ipAddress = null;
			while (allNetInterfaces.hasMoreElements()){
				NetworkInterface netInterface =  allNetInterfaces.nextElement();
				Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
				while (addresses.hasMoreElements()){
					ipAddress = (InetAddress) addresses.nextElement();
					if (ipAddress != null && !ipAddress.isLoopbackAddress() && ipAddress instanceof Inet4Address){
						ip =  ipAddress.getHostAddress();
						return ip;
					} 
				}
			}
		}catch(Exception ex){
			
		}
        
		return ip;
	}
	public static String getHostName(InetAddress netAddress){
		if(null == netAddress){
			return null;
		}
		String name = netAddress.getHostName(); //get the host address
		return name;
	}

	public static boolean isLinux(){
		
		boolean isLinux = false;
		String os = System.getProperty("os.name"); 
		if(Strings.isNullOrEmpty(os))
			 isLinux = false;
		if(os.toLowerCase().startsWith("win"))	
			isLinux = false;
		if(os.toLowerCase().startsWith("linux"))
			isLinux = true;
		return isLinux;
	}

}

