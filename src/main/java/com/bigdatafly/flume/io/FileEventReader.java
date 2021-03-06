/**
 * 
 */
package com.bigdatafly.flume.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.log.DefaultLog4jParser;
import com.bigdatafly.flume.log.ILog4jParser;
import com.bigdatafly.flume.log.LogEntry;
import com.bigdatafly.flume.log.LogEvent;
import com.bigdatafly.flume.utils.OSUtils;


/**
 * @author summer
 *
 */
public class FileEventReader implements  EventReader{

	private static final Logger logger = LoggerFactory
			.getLogger(FileEventReader.class);
	
	private File monitorFile;
	private int  capacity;
	private int bufSize;
	private File positionTrackerFile;
	private PositionTracker positionTracker;
	private ILog4jParser parser;

	private ByteBuffer rBuffer;
	
	private static final int DEFAULT_CAPACITY = 200;  
	private static final int DEFAULT_BUF_SIZE = 512;
	private static final String DEFAULT_POSITION_TRACKER_FILE_PATH = "/tmp";
	
	static final String ip_addr=OSUtils.getHostIp(OSUtils.getInetAddress());
	static final String host_name = OSUtils.getHostName(OSUtils.getInetAddress());
	
	static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
	
	private Charset charset;
	
	public FileEventReader(File monitorFile){
		
		this(monitorFile,DEFAULT_CAPACITY);
	}
	
	public FileEventReader(File monitorFile,int capacity){
		
		this(monitorFile,getDefaultPositionTrackerFile() ,capacity,DEFAULT_BUF_SIZE);
		
	}
	
	public FileEventReader(File monitorFile,File positionTrackerFile){
		
		this(monitorFile,positionTrackerFile,DEFAULT_CAPACITY,DEFAULT_BUF_SIZE);
	}
	
	public FileEventReader(File monitorFile,File positionTrackerFile,int capacity,int bufSize){
		
		this(monitorFile,positionTrackerFile,capacity,bufSize,DEFAULT_CHARSET);
		
	}
	
	public FileEventReader(File monitorFile,File positionTrackerFile,int capacity,int bufSize,Charset charset){
		
		this.monitorFile = monitorFile;
		this.parser = createDefaultLog4jParser();
		this.capacity = capacity>0?capacity:DEFAULT_BUF_SIZE;
		this.positionTrackerFile = positionTrackerFile;
		this.positionTracker = getDefaultPositionTracker(this.positionTrackerFile);
		this.bufSize = bufSize>0?bufSize:DEFAULT_BUF_SIZE;
		this.rBuffer = ByteBuffer.allocate(bufSize);
		this.charset = charset;
	}
	
	
	protected ILog4jParser createDefaultLog4jParser(){
		
		return new DefaultLog4jParser();
	}
	
	static  PositionTracker getDefaultPositionTracker(File file){
		
		PositionTracker positionTracker = new PositionTracker(file);
		positionTracker.init();
		return positionTracker;
	}
	
	static  PositionTracker getDefaultPositionTracker(String file){
		
		PositionTracker positionTracker = new PositionTracker(file);
		positionTracker.init();
		return positionTracker;
	}
	
	
	
	public PositionTracker getPositionTracker() {
		return positionTracker;
	}

	public List<Event> readEvents() {
		
		List<Event> events = new ArrayList<Event>();
		RandomAccessFile coreFile = null;
		
		try{
			coreFile = new RandomAccessFile(monitorFile, "r");					
			FileChannel coreFileChannel = coreFile.getChannel();
			List<LogEntry> logEntries = readLogEntry(coreFileChannel,positionTracker,rBuffer);
			
			for(LogEntry logEntry : logEntries){
				
				Event event = convert(logEntry);
				events.add(event);
			}
			
		}catch(FileNotFoundException ex){
			
			if(logger.isDebugEnabled())
				logger.debug("monitorFile FileNotFoundException", ex);
			//return null;
		}finally{
			try {
				
				if(coreFile!=null)
					coreFile.close();
			} catch (IOException e) {
				
				if(logger.isDebugEnabled())
					logger.debug("monitorFile close throw IOException", e);
			}
		}
		return events;	
	}

	/**
	 * 
	 * @param logEntry
	 * @return
	 * 
	 * 协议头
	 * 
	 * hostname
	 * ip
	 * log length
	 * log level
	 * log time
	 * 
	 * 消息体    长度
	 * 
	 * 消息长度  4
	 * ip    15
	 * log
	 */
	
	public Event convert(final LogEntry logEntry) {
		
		Map<String,String> headers = new HashMap<String,String>();
		headers.put(LogEvent.LOG_LEVEL_KEY, logEntry.getLevel());
		headers.put(LogEvent.LOG_TIME_KEY, logEntry.getLogtime());
		headers.put(Constants.HOST_NAME_HEADER, host_name);
		int dataLen = 0;
		String log = logEntry.getLog();
		dataLen = (log==null)?0:log.length();
		headers.put(Constants.FLOW_COUNT_HEADER, String.valueOf(dataLen));
		
		LogEvent logEvent = new LogEvent();
		logEvent.setIp(ip_addr);
		logEvent.setLen(dataLen);
		logEvent.setLevel(logEntry.getLevel());
		logEvent.setLogtime(logEntry.getLogtime());
		logEvent.setLog(log);
		
		return EventBuilder.withBody(logEvent.toString().getBytes(charset), headers);
	}
	
	private List<LogEntry> readLogEntry(FileChannel fcin, PositionTracker positionTracker,ByteBuffer rBuffer){
		
		return readLogFile(fcin,positionTracker,rBuffer);
	}
	
	private  List<LogEntry> readLogFile(FileChannel fcin, PositionTracker positionTracker,ByteBuffer rBuffer){ 
		
		List<LogEntry> logEntries =new ArrayList<LogEntry>();
		byte[] bs = new byte[bufSize]; 
		int buf_size = 0; 
		long position = positionTracker.getPosition();
		StringBuffer strBuf = new StringBuffer(""); 
		int log_capacity = 0;
		try{ 
			
			fcin.position(position);
			
			while((buf_size=fcin.read(rBuffer)) != -1){ 
				  
				 if(log_capacity > capacity || buf_size == 0){
					rBuffer.clear(); 
					break;
				 }
				
				 
			      int rSize = rBuffer.position(); 
			      //rBuffer.rewind(); 
			      rBuffer.flip(); 
			      rBuffer.get(bs,0,rSize); 
			      rBuffer.clear(); 
			      String tempString = new String(bs, 0, rSize); 
			     
			      
				  strBuf.append(tempString);
				  List<LogEntry> logs = parser.parse(strBuf); 
				  logEntries.addAll(logs);
				  log_capacity =  logEntries.size();
			     
			      position += buf_size;
			     
			      //positionTracker.mark(position);
			      positionTracker.mark(position - strBuf.length());
			      //positionTracker.save();
			}	
		
		} catch (IOException e) { 
			
			e.printStackTrace(); 
			positionTracker.reset();
			//positionTracker.save();
		} 
		
		return logEntries;
		
	}
	
	
	
	static File getDefaultPositionTrackerFile(){
		
		return new File(DEFAULT_POSITION_TRACKER_FILE_PATH); 
	}
	
	public static class Builder{
		
		private File monitorFile;
		private int  capacity;
		private int bufSize;
		private File positionTrackerFile;
		private Charset charset;
		
		public  Builder(){
			
			this.capacity = DEFAULT_CAPACITY;
			this.bufSize = DEFAULT_BUF_SIZE;
			this.positionTrackerFile = getDefaultPositionTrackerFile(); 
			this.charset = Charset.defaultCharset();
		}
		
		public Builder setMonitorFile(File monitorFile){
			
			this.monitorFile = monitorFile;
			return this;
		}
		
		public  Builder setCapacity(int  capacity){
			
			this.capacity = capacity;
			return this;
		}
		
		public  Builder setBufSize(int  bufSize){
			
			this.bufSize = bufSize;
			return this;
		}
		
		public  Builder setPositionTrackerFile(File  positionTrackerFile){
			
			this.positionTrackerFile = positionTrackerFile;
			return this;
		}
		
		public Builder setCharset(Charset charset) {
			
			this.charset = charset;
			return this;
		}
		
		
		public FileEventReader builder(){
			return new FileEventReader(monitorFile,positionTrackerFile,capacity,bufSize,charset);
		}

		
	}

}
