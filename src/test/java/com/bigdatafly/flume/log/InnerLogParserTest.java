/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.Map;

import com.bigdatafly.flume.log.log4j.InnerLogParser;

/**
 * @author summer
 *
 */
public class InnerLogParserTest {

	public static void main(String[] args) throws Exception{
		
		final String conversionPattern = "%d{yyyy-MM-dd HH:mm:ss,SSS} %p %c - %m%n";
		InnerLogParser logparser = new InnerLogParser(conversionPattern);
		
		StringBuffer sb = new StringBuffer();
		sb.append("2016-05-14 08:10:20,276 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x253652694d40111 after 0ms");
		sb.append("2016-05-14 08:10:40,699 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x353652694e3010d after 0ms");
		sb.append("2016-05-14 08:11:00,314 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x253652694d40111 after 0ms");
		for(Map.Entry<String, Object> entry :logparser.fetchARecord(sb).entrySet()){
			
			;//System.out.println(entry.getKey() + "====>" + entry.getValue());
		}
	}
}
