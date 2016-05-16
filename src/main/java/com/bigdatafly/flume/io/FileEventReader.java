/**
 * 
 */
package com.bigdatafly.flume.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.bigdatafly.flume.common.DataVo;
import com.bigdatafly.flume.common.LogEntry;

/**
 * @author summer
 *
 */
public class FileEventReader implements  EventReader{

	private int capacity;
	
	public  DataVo readFileByLine(int bufSize, FileChannel fcin, ByteBuffer rBuffer){ 
		String enterStr = "\n"; 
		try{ 
		byte[] bs = new byte[bufSize]; 

		int size = 0; 
		StringBuffer strBuf = new StringBuffer(""); 
		int buf_size=0;
		int line_count=0;
		//while((size = fcin.read(buffer)) != -1){ 
		List<LogEntry> lineList=new ArrayList<LogEntry>();
		while(fcin.read(rBuffer) != -1){ 
		      int rSize = rBuffer.position(); 
		      rBuffer.rewind(); 
		      rBuffer.get(bs); 
		      rBuffer.clear(); 
		      String tempString = new String(bs, 0, rSize); 
		      buf_size+=tempString.getBytes().length;
		      
		      strBuf.append(tempString);
		      
		      int fromIndex = 0; 
		      int endIndex = 0; 
		      
		      while((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1){ 
		       String line = tempString.substring(fromIndex, endIndex); 
		       line = new String(strBuf.toString() + line); 
		       strBuf.delete(0, strBuf.length()); 
		       fromIndex = endIndex + 1; 	
		       size+=line.getBytes().length+1;
		       line_count++;
		       //lineList.add(line);
		       if(line_count>capacity){
		    	   DataVo vo =new DataVo();
		    	   vo.setDataSize(size);
		    	   //vo.setLineList(lineList);
		    	   return vo;
		       }
		      } 
		      if(rSize > tempString.length()){ 
		      strBuf.append(tempString.substring(fromIndex, tempString.length())); 
		    //  size+=strBuf.toString().getBytes().length;
		      }else{ 
		      strBuf.append(tempString.substring(fromIndex, rSize)); 
		    //  size+=strBuf.toString().getBytes().length;
		      } 
		     
		    
		}
		 System.out.println("**********end**********"+size);
		 if(lineList.size()>0){
			  DataVo vo =new DataVo();
	    	   vo.setDataSize(size);
	    	   //vo.setLineList(lineList);
	    	   return vo;
    	   }else
    		   return null;
		
		} catch (IOException e) { 
		// TODO Auto-generated catch block 
		e.printStackTrace(); 
			return null;
		} 
		
	}
	
	private List<LogEntry> parseLogEntry(StringBuffer sb){
		
		List<LogEntry> logEntries =new ArrayList<LogEntry>();
		
		int beginIndex = 0;
		int endIndex = 0;
		
		while(true){
				String tempStr = sb.toString();
				int len = tempStr.length();
				
				if(len > LogEntry.LOG_LEVEL_LEN){
					beginIndex = StringUtils.indexOfAny(tempStr, LogEntry.logLevels);
					
					if(beginIndex <0){
						sb.delete(0, len);
						break;
					}
					else{
						String temp =sb.substring(LogEntry.LOG_LEVEL_LEN+beginIndex);
						endIndex = StringUtils.indexOfAny(temp, LogEntry.logLevels);
						if(endIndex <0){
							break;
						}else{
							
							String strLogEntry = StringUtils.mid(tempStr, beginIndex, LogEntry.LOG_LEVEL_LEN+endIndex);
							System.out.println("{"+strLogEntry+"}");
							sb.delete(0, beginIndex + LogEntry.LOG_LEVEL_LEN+endIndex);
						}
					}
					
				}else{
					break;
				}
				
		}
		
		
		return logEntries;
	}
	
	public static void main(String[] args){
		
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
		new FileEventReader().parseLogEntry(sb);
		System.out.println("******************************************");
		System.out.println(sb);
	}
	
}
