/**
 * 
 */
package com.bigdatafly.flume.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author summer
 *
 */
public class RegexUtils {

	public static final String NUMERIC_REGEX = "^([-|+]?\\d+)(\\.\\d+)?$";
	
	public static boolean isNumeric(String val){
		return matcher(NUMERIC_REGEX,val);
	}
	
	public static boolean matcher(String regex,Object val){
		
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(String.valueOf(val));
		return m.matches();
	}
	
	
	public static void main(String[] args) throws Exception{
		
		System.out.println(isNumeric(null)==false);
		System.out.println(isNumeric("312")==true);
		System.out.println(isNumeric("")==false);
		System.out.println(isNumeric("1231.1231.3123")==false);
		System.out.println(isNumeric("234sfs123")==false);
		System.out.println(isNumeric("-0.123")==true);
		System.out.println(isNumeric("0.123")==true);
		System.out.println(isNumeric("12312.123")==true);
		System.out.println(isNumeric("+12312.123")==true);
		System.out.println(isNumeric(".123")==false);
	}
}
