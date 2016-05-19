/**
 * 
 */
package com.bigdatafly.flume.io;

import java.nio.charset.Charset;

/**
 * @author summer
 *
 */
public class StringTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	    String ip = "192.168.100.000";
		System.out.println(ip.getBytes(Charset.forName("UTF-8")).length);
		
		ip="0.0.0.0";
		byte[] arr = new byte[15];
		
		System.out.println(ip.getBytes());
		System.arraycopy(ip.getBytes(), 0, arr, 0, ip.getBytes().length);
		
		System.out.println(new String(arr));
		
		StringBuffer body = new StringBuffer();
		body.append("123123");
		body.append("中国");
		int length = body.length();
		char[] dst = new char[length];
		
		body.getChars(0, length, dst, 0);
		
		System.out.println(dst);
		
		
		body.toString().getBytes();

	}

}
