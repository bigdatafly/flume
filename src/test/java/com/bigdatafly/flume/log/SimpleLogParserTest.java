/**
 * 
 */
package com.bigdatafly.flume.log;

import java.util.List;

/**
 * @author summer
 *
 */
public class SimpleLogParserTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SimpleLog4jParser parser = new SimpleLog4jParser();
		
		StringBuffer sb = new StringBuffer();
		sb.append("2016-05-14 08:10:20,276 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x253652694d40111 after 0ms");
		sb.append("2016-05-14 08:10:40,699 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x353652694e3010d after 0ms");
		sb.append("2016-05-14 08:11:00,314 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x253652694d40111 after 0ms");
		sb.append("2016-05-14 08:11:00,314 DEBUG [org.apache.zookeeper.ClientCnxn] - Got ping response for sessionid: 0x253652694d40111 after 0ms");
		List<LogEntry> entries = parser.parseLogEntry(sb);
		
		System.out.println(entries);

	}

}
