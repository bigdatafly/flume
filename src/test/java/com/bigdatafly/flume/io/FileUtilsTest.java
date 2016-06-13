/**
 * 
 */
package com.bigdatafly.flume.io;

import java.io.File;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.log4j.lf5.LogLevel;

import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.log.DefaultLog4jParser;
import com.bigdatafly.flume.log.LogEntry;
import com.bigdatafly.flume.utils.JsonUtils;
import com.google.common.collect.Maps;


/**
 * @author summer
 *
 */


public class FileUtilsTest {
	
	
	public static void testIndexOf(){
		
		List<String> logTypes = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		List<LogLevel> levels = LogLevel.getLog4JLevels();
		for(LogLevel level : levels)
			logTypes.add((String.format("[%-5s]", level)));
		
		String s = "Size of avail pool [INFO ] [09:09:35] SockIOPool:1578- ++++ Size of avail pool for host (172.16.15.80:11215) = 5";
		int pos = StringUtils.indexOfAny(s,logTypes.toArray(new String[0]) );
		System.out.println(pos);
		
	}

	public void testParseLogEntry(){
		
		StringBuffer sb = new StringBuffer();
		sb.append("23232[DEBUG] [09:09:23] ClientCnxn:756- Got ping response for sessionid: 0x453a360b3c6007f after 0ms");
		sb.append("111[DEBUG] [09:09:35] SockIOPool:1529- ++++ Size of avail pool for host (172.16.15.80:11215) = 5");
		sb.append("[INFO ] [09:09:00] MemCachedClient:1588- ++++ deserializing class com.odianyun.sc.model.dto.output.DomainInfoDTO");
		sb.append("[DEBUG] [09:08:55] RequestMappingHandlerMapping:216- Returning handler method [public java.util.Map<java.lang.String, java.lang.Object> com.odianyun.back.merchant.web.write.action.enterpriseQualifications.EnterpriseQualificationsController.findEnterpriseQualificationsAuditStatus()]");
		sb.append("[ERROR] [09:06:50] ExceptionFilter:156- "+"\n"+
				"				  org.springframework.web.util.NestedServletException: Request processing failed; nested exception is java.lang.reflect.UndeclaredThrowableException"+"\n"+
				"at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:927)"+"\n"+
				"at org.springframework.web.servlet.FrameworkServlet.doPost(FrameworkServlet.java:822)");
		sb.append("[DEBUG] [09:09:23] ClientCnxn:756- Got ping response for sessionid: 0x453a360b3c6007f after 0ms");
		sb.append("[DEBUG] [09:09:23] ClientCnxn:756- Got ping response for sessionid: 0x453a360b3c6007f after 0ms");
		sb.append("[DEBUG] [09:09:23] ClientCnxn:756- Got ping response for sessionid: 0x453a360b3c6007f after 0ms");
		for(LogEntry e :new DefaultLog4jParser().parse(sb))
			System.out.println(e);;
		System.out.println("******************************************");
		System.out.println(sb);
	}
	
	static Map<String,Integer> stats = Maps.newHashMap();
	
	public void testReadEvents(){
		
		File monitorFile = new File("E:\\basics-merchant-service_20160514_091006\\catalina.out");
		File positionTrackerFilePath = new File("E:\\basics-merchant-service_20160514_091006",Constants.POSITION_FILE_NAME);
		int capacity = 100;
		
		stats.clear();
		
		FileEventReader.Builder builder = new  FileEventReader.Builder();
		EventReader reader = builder
				.setMonitorFile(monitorFile)
				.setPositionTrackerFile(positionTrackerFilePath)
				.setCapacity(capacity)
				.setBufSize(512)
				.builder(); 
		PositionTracker positionTracker = reader.getPositionTracker();
		
		String hostName;
		String logTime;
		String key;
		String formatLogTime;
		int    totalEvents = 0;
		
		while(true){
			List<Event> events = reader.readEvents();
			if(events == null || events.isEmpty())
				break;
			System.out.println("{eventnumber:"+events.size()+",PositionTracker:"+positionTracker);
			totalEvents += events.size();
			for(Event e : events){
				Map<String,Object> eventMap = toMap(e);
				hostName = eventMap.get("ip").toString();
				logTime =  eventMap.get("logtime").toString();
				formatLogTime = toDate0(logTime);
				
//				if(StringUtils.isEmpty(logTime) || 
//						StringUtils.isEmpty(formatLogTime) ||
//						logTime.length() != 23 ||
//						formatLogTime.length() <12){
//					System.out.println("error => {logtime:" + logTime +",formatLogTime:"+formatLogTime);
//					
//				}
				
				key = "3032"+ formatLogTime;
				if(stats.containsKey(key))
					stats.put(key, stats.get(key) +1);
				else
					stats.put(key, 1);
				/*
				byte[] body = e.getBody();
				String s = new String(body);
				
				if(s!=null && s.indexOf("DEBUG")<0){
					System.out.println(s);
					System.out.println("------------------------------------------------------------");
					
				}*/
			}
		}
		System.out.println("------------------------------------------------------------");
		
		System.out.println("----------------------totalEvents:"+totalEvents+"-----------------------------------");
		
		for(Map.Entry<String, Integer> e : stats.entrySet()){
			
			System.out.println(e.getKey()+"==================>"+e.getValue());
		}
	}
	
	private Map<String,Object> toMap(Event event){
		
		byte[] body = event.getBody();
		if(body == null)
			return Maps.newHashMap();
		@SuppressWarnings("unchecked")
		Map<String,Object> map = JsonUtils.fromJson(new String(body,Charset.defaultCharset()), Map.class);
		return map;
	}
	
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
	
	public static void main(String[] args){
		
		
		//new FileUtilsTest().testParseLogEntry();
		
		new FileUtilsTest().testReadEvents();
		
		
	}

}
