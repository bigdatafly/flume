/**
 * 
 */
package com.bigdatafly.flume.io;

import java.io.File;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventHelper;

import com.bigdatafly.flume.log.LogEntry;

/**
 * @author summer
 *
 */


public class FileUtilsTest {

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
		for(LogEntry e :new FileEventReader(null).parseLogEntry(sb))
			System.out.println(e);;
		System.out.println("******************************************");
		System.out.println(sb);
	}
	
	
	public void testReadEvents(){
		
		File monitorFile = new File("E:\\back-merchant-web_20160514_091002\\catalina.out");
		File positionTrackerFilePath = new File("E:\\back-merchant-web_20160514_091002");
		int capacity = 100;
		EventReader reader = new FileEventReader
				.Builder()
				.setMonitorFile(monitorFile)
				.setPositionTrackerFile(positionTrackerFilePath.getAbsolutePath())
				.setCapacity(capacity)
				.setBufSize(512)
				.builder(); 
		PositionTracker positionTracker = reader.getPositionTracker();
		while(true){
			List<Event> events = reader.readEvents();
			if(events == null || events.isEmpty())
				break;
			//System.out.println("{eventnumber:"+events.size()+",PositionTracker:"+positionTracker);
			for(Event e : events){
				
				byte[] body = e.getBody();
				String s = new String(body);
				if(s!=null && s.indexOf("[ERROR]")>-1){
					System.out.println(s);
					System.out.println("------------------------------------------------------------");
				}
			}
		}
	}
	
	
	public static void main(String[] args){
		
		
		//new FileUtilsTest().testParseLogEntry();
		
		new FileUtilsTest().testReadEvents();
		
		
	}
}
