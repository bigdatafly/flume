/**
 * 
 */
package com.bigdatafly.flume.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Sets;



/**
 * @author summer
 *
 */
public class Utils {

	public static final String DATE_FORMAT_1 = "yyyy-MM-dd HH:mm:ss,SSS";
	public static final String DATE_FORMAT_2 = "[HH:mm:ss]";
	
	public static final Set<String> DATA_FORMATS = Sets.newHashSet();
	static{
		DATA_FORMATS.add(DATE_FORMAT_1);
		DATA_FORMATS.add(DATE_FORMAT_2);
	};
	
	public static String toDate0(String date){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		if(StringUtils.isEmpty(date))
			return sdf.format(new Date());
		int pos = date.lastIndexOf(":");
		if(pos == -1)
			return sdf.format(new Date());
		date = date.substring(0,pos);
		return date.replaceAll("[\\s|:|-]+", "");
		
	}
	
	public static boolean isDate(String date){
		if(StringUtils.length(date) ==0)
			return false;
		return RegexUtils.matcher("^(^(\\d{4}|\\d{2})(\\-|\\/|\\.)\\d{1,2}\\3\\d{1,2}( (\\d{1}|\\d{2}):(\\d{1}|\\d{2}):(\\d{1}|\\d{2})(,\\d{3})?)?)$", date);
	}
	
	public static boolean isTime(String time){
		
		if(StringUtils.length(time) ==0)
			return false;
		return RegexUtils.matcher("(\\[)?\\d{1,2}:\\d{1,2}:\\d{1,2}(\\])?",time);
	}
	
	public static void main(String[] args) throws Exception{
		
		System.out.println(Utils.isDate("2016-05-13 23:10:13,382"));
		System.out.println(Utils.isDate("16-05-13 23:1:15"));
		System.out.println(Utils.isDate("16-05-13"));
		System.out.println(Utils.isDate("16-05-1"));
		System.out.println(Utils.isDate("16-5-1"));
		System.out.println(Utils.isDate("16-501"));
		
		System.out.println(Utils.isTime("16:5:01"));
		System.out.println(Utils.isTime("[16:5:01]"));
		
		System.out.println(Utils.isTime("[16:05:011]"));
		System.out.println(Utils.isTime("[16:05:011]"));
		System.out.println(Utils.isTime("[1605:011]"));
	}
}
