/**
 * 
 */
package com.bigdatafly.flume.utils;

import com.google.gson.Gson;

/**
 * @author summer
 *
 */
public class JsonUtils {

	public static String toJson(Object obj){
		
		
		try{
			Gson gson = new Gson();
			return gson.toJson(obj);
		}catch(Exception ex){
			return "";
		}
	}
	
	public static <T> T fromJson(String json,Class<T> type) {
		
		try{
			Gson gson = new Gson();
			return gson.fromJson(json, type);
		}catch(Exception ex){
			return null;
		}
	}
}
