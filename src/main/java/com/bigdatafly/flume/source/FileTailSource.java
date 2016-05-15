package com.bigdatafly.flume.source;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.utils.OSUtils;

public class FileTailSource extends AbstractSource implements Configurable,
		EventDrivenSource {

	private static final Logger log = LoggerFactory
			.getLogger(FileTailSource.class);
	private ChannelProcessor channelProcessor;
	private RandomAccessFile monitorFile = null;
	private File coreFile = null;
	private long lastMod = 0L;
	private String monitorFilePath = null;
	private String positionFilePath = null;
	private FileChannel monitorFileChannel = null;
	private ByteBuffer buffer = ByteBuffer.allocate(500);// 1MB
	private long positionValue = 0L;
	private ScheduledExecutorService executor;
	private FileMonitorThread runner;
	private PositionLog positionLog = null;
	private Charset charset = null;
	private CharsetDecoder decoder = null;
	private CharBuffer charBuffer = null;
	private long counter = 0L;
	private Map<String, String> headers = new HashMap<String, String>();// event
																		// //
																		// header
	private Object exeLock = new Object();
	private long lastFileSize = 0L;
	private long nowFileSize = 0L;
	private SourceCounter sourceCounter;
	Integer capacity = null;
	int defaultCapacity = 100;
	public static int commitSize=0;
	
	
	@Override
	public synchronized void start() {
		channelProcessor = getChannelProcessor();
		executor = Executors.newSingleThreadScheduledExecutor();
		runner = new FileMonitorThread(capacity);
		executor.scheduleWithFixedDelay(runner, 500, 2000,
				TimeUnit.MILLISECONDS);
		sourceCounter.start();
		
		super.start();
		log.debug("FileMonitorSource source started");
	}

	@Override
	public synchronized void stop() {
		positionLog.setPosition(positionValue);
		log.debug("Set the positionValue {} when stopped", positionValue);
		if (this.monitorFileChannel != null) {
			try {
				this.monitorFileChannel.close();
			} catch (IOException e) {
				log.error(
						this.monitorFilePath + " filechannel close Exception",
						e);
			}
		}
		if (this.monitorFile != null) {
			try {
				this.monitorFile.close();
			} catch (IOException e) {
				log.error(this.monitorFilePath + " file close Exception", e);
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			log.info("Interrupted while awaiting termination", ex);
		}
		executor.shutdownNow();
		sourceCounter.stop();
		
		super.stop();
		log.debug("FileMonitorSource source stopped");
	}

	@Override
	public void configure(Context context) {
		charset = Charset.defaultCharset();
		decoder = charset.newDecoder();
		decoder.onMalformedInput(CodingErrorAction.IGNORE);
		this.monitorFilePath = context.getString(Constants.MONITOR_FILE);
		this.positionFilePath = context.getString(Constants.POSITION_DIR);
		try {
			capacity = context.getInteger("capacity", defaultCapacity);
		} catch (NumberFormatException e) {
			capacity = defaultCapacity;
		}
		Preconditions.checkArgument(monitorFilePath != null,
				"The file can not be null !");
		Preconditions.checkArgument(positionFilePath != null,
				"the positionDir can not be null !");
		log.debug("monitorFilePath:" + monitorFilePath + " positionFilePath:"
				+ positionFilePath);
		if (positionFilePath.endsWith(":")) {
			positionFilePath += File.separator;
		} else if (positionFilePath.endsWith("\\")
				|| positionFilePath.endsWith("/")) {
			positionFilePath = positionFilePath.substring(0,
					positionFilePath.length() - 1);
		}
		File file = new File(positionFilePath + File.separator
				+ Constants.POSITION_FILE_NAME);
		if (!file.exists()) {
			try {
				file.createNewFile();
				log.debug("Create the {} file", Constants.POSITION_FILE_NAME);
			} catch (IOException e) {
				log.error("Create the position.properties error", e);
				return;
			}
		}
		try {
			coreFile = new File(monitorFilePath);
			lastMod = coreFile.lastModified();
		} catch (Exception e) {
			log.error("Initialize the File/FileChannel Error", e);
			return;
		}

		positionLog = new PositionLog(positionFilePath);
		try {
			positionValue = positionLog.initPosition();
		} catch (Exception e) {
			log.error("Initialize the positionValue in File positionLog", e);
			return;
		}
		lastFileSize = positionValue;
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
		
        
	}
   class DataVo {
	   long dataSize;
	   List<String> lineList;
	public long getDataSize() {
		return dataSize;
	}
	public void setDataSize(long dataSize) {
		this.dataSize = dataSize;
	}
	public List<String> getLineList() {
		return lineList;
	}
	public void setLineList(List<String> lineList) {
		this.lineList = lineList;
	}
	   
   }
	class FileMonitorThread implements Runnable {
		int capacity = 0;

		public FileMonitorThread(int capacity) {
			this.capacity = capacity;
		}
		public  DataVo readFileByLine(int bufSize, FileChannel fcin, ByteBuffer rBuffer){ 
			String enterStr = "\n"; 
			try{ 
			byte[] bs = new byte[bufSize]; 

			int size = 0; 
			StringBuffer strBuf = new StringBuffer(""); 
			int buf_size=0;
			int line_count=0;
			//while((size = fcin.read(buffer)) != -1){ 
			List<String> lineList=new ArrayList<String>();
			while(fcin.read(rBuffer) != -1){ 
			      int rSize = rBuffer.position(); 
			      rBuffer.rewind(); 
			      rBuffer.get(bs); 
			      rBuffer.clear(); 
			      String tempString = new String(bs, 0, rSize); 
			      buf_size+=tempString.getBytes().length;
			     /// System.out.println(tempString);
			      int fromIndex = 0; 
			      int endIndex = 0; 
			      while((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1){ 
			       String line = tempString.substring(fromIndex, endIndex); 
			       line = new String(strBuf.toString() + line); 
			       strBuf.delete(0, strBuf.length()); 
			       fromIndex = endIndex + 1; 	
			       size+=line.getBytes().length+1;
			       line_count++;
			       lineList.add(line);
			       if(line_count>capacity){
			    	   DataVo vo =new DataVo();
			    	   vo.setDataSize(size);
			    	   vo.setLineList(lineList);
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
	    	   vo.setLineList(lineList);
	    	   return vo;
	    	   }else
	    		   return null;
			
			} catch (IOException e) { 
			// TODO Auto-generated catch block 
			e.printStackTrace(); 
			return null;
			} 
			
		} 
		/**
		 * a thread to check whether the file is modified
		 */
		@Override
		public void run() {
			synchronized (exeLock) {
				System.out.println("FileMonitorThread running ..." + capacity+"****eventSize****"+commitSize);
				// coreFile = new File(monitorFilePath);
				long nowModified = coreFile.lastModified();
				log.debug(nowModified + ":" + lastMod);
				// the file has been changed
				if (lastMod != nowModified) {
					log.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>File modified ...");
					lastMod = nowModified;
					nowFileSize = coreFile.length();
					int readDataBytesLen = 0;
					try {
						log.debug(
								"The Last coreFileSize {},now coreFileSize {}",
								lastFileSize, nowFileSize);
						// it indicated the file is rolled by log4j
						if (nowFileSize <= lastFileSize) {
							log.debug("The file size is changed to be lower,it indicated that the file is rolled by log4j.");
							positionValue = 0L;
						}
						lastFileSize = nowFileSize;
						monitorFile = new RandomAccessFile(coreFile, "r");					
						monitorFile.readLine();
						monitorFileChannel = monitorFile.getChannel();
						
						monitorFileChannel.position(positionValue);
						DataVo datavo = readFileByLine(500, monitorFileChannel, buffer);
						while(null!=datavo){
							positionValue += datavo.getDataSize();
							positionLog.setPosition(positionValue); // �����Ѿ���ȡ�����µ�ַ
							monitorFileChannel.position(positionValue);
					
							channelProcessor .processEventBatch(getEventByListData(datavo.getLineList()));
							System.out.println("processEventBatch*************end*****************");
							datavo = readFileByLine(500, monitorFileChannel, buffer);
						}

					} catch (Exception e) {
						e.printStackTrace();
						log.error("Read data into Channel Error", e);
						log.debug(
								"Save the last positionValue {} into Disk File",
								positionValue - readDataBytesLen);
						positionLog.setPosition(positionValue
								- readDataBytesLen);
					}
					counter++;
					if (counter % Constants.POSITION_SAVE_COUNTER == 0) {
						log.debug(
								Constants.POSITION_SAVE_COUNTER
										+ " times file modified checked,save the position Value {} into Disk file",
								positionValue);
						positionLog.setPosition(positionValue);
					}
					
					
				}
			}
		}

	}

	
	public List<Event> getEventByReadData(String readData) {
		String str[] = readData.split("\n");
		int len = str.length;
		List<Event> events = new ArrayList<Event>();
		for (int i = 0; i < len; i++) {
			Event event = EventBuilder.withBody((str[i]).getBytes());
			events.add(event);
		}
		return events;
	}

	public List<Event> getEventByListData(List<String> listData) {
		List<Event> events = new ArrayList<Event>();
		String ip_addr=OSUtils.getHostIp(OSUtils.getInetAddress());
		String host_name = OSUtils.getHostName(OSUtils.getInetAddress());
		long dataLen = 0;
		for (String data:listData) {
		    LogInfo log=new LogInfo();
		    log.setBody(data);
		    log.setHostName(ip_addr);
			Event event = EventBuilder.withBody(ObjectToByte(log));
			Map<String,String> headers = event.getHeaders();
			if(!headers.containsKey(Constants.HOST_NAME_HEADER))
				headers.put(Constants.HOST_NAME_HEADER, host_name);
			
			if(!headers.containsKey(Constants.FLOW_COUNT_HEADER)){
				if(!StringUtils.isEmpty(data))
					dataLen = data.length();
				headers.put(Constants.FLOW_COUNT_HEADER, String.valueOf(dataLen));
			}
			events.add(event);
		}
		//EventBuilder.withBody(body, headers)
		return events;
	}
	
	public static byte[] ObjectToByte(LogInfo obj) {
		byte[] bytes = null;
		try {
			// object to bytearray
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);
			bytes = bo.toByteArray();
			bo.close();
			oo.close();
		} catch (Exception e) {
			System.out.println("translation" + e.getMessage());
			e.printStackTrace();
		}
		return bytes;
	}
	
	
	public String buffer2String(ByteBuffer buffer) {

		buffer.flip();

		try {
			// log.debug("size:************"+buffer.limit());
			if (buffer.limit() <= 0)
				return "";
			CharBuffer cb = decoder.decode(buffer);
			// decoder.decode(buffer, charBuffer, true);
			// charBuffer = decoder.decode(buffer);
			buffer.flip();
			// log.debug(cb.toString());
			return cb.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}

}
