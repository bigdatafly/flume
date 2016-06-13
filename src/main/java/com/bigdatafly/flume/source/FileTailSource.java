package com.bigdatafly.flume.source;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.io.EventReader;
import com.bigdatafly.flume.io.FileEventReader;
import com.bigdatafly.flume.io.PositionTracker;
import com.google.common.base.Preconditions;

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
	//private PositionLog positionLog = null;
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
	private EventReader reader;
	private PositionTracker positionTracker; 
	
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
		
		this.positionTracker.save();
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
		/*
		positionLog = new PositionLog(positionFilePath);
		try {
			positionValue = positionLog.initPosition();
		} catch (Exception e) {
			log.error("Initialize the positionValue in File positionLog", e);
			return;
		}
		*/
		this.reader = new FileEventReader
				.Builder()
				.setMonitorFile(coreFile)
				.setPositionTrackerFile(file)
				.setCapacity(capacity)
				.setBufSize(512)
				.setCharset(charset)
				.builder();
		
		this.positionTracker = this.reader.getPositionTracker();
		positionValue = positionTracker.getPosition();
		
		lastFileSize = positionValue;
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
		
        
	}
	
	
	class FileMonitorThread implements Runnable {
		int capacity = 0;

		public FileMonitorThread(int capacity) {
			this.capacity = capacity;
		}

		/**
		 * a thread to check whether the file is modified
		 */
		
		public void run() {
			synchronized (exeLock) {
				//System.out.println("FileMonitorThread running ..." + capacity+"****eventSize****"+commitSize);
				// coreFile = new File(monitorFilePath);
				long nowModified = coreFile.lastModified();
				
				// the file has been changed
				if (lastMod != nowModified) {
					
						lastMod = nowModified;
						nowFileSize = coreFile.length();

						log.debug(
								"The Last coreFileSize {},now coreFileSize {}",
								lastFileSize, nowFileSize);
						// it indicated the file is rolled by log4j
					    if (nowFileSize <= lastFileSize) {
									log.debug("The file size is changed to be lower,it indicated that the file is rolled by log4j.");
									positionValue = 0L;
						}
						lastFileSize = nowFileSize;

						 while(true){
									
							    List<Event> events = reader.readEvents();
							    
							    log.debug("*********************event:"+events+"*************************");
							    
							    if(events ==null || events.isEmpty()){ 
							    	commitSize = 0;
									break;
							    }
							    
							    try{
								    commitSize = events.size();
									channelProcessor.processEventBatch(events);
									/**
									 * modified at 2016-6-13 17:51 
									 */
									reader.getPositionTracker().save();
									/**
									 *  modified at 2016-6-13 17:51 
									 */
									sourceCounter.addToEventAcceptedCount(commitSize);
									sourceCounter.incrementAppendBatchAcceptedCount();
							    }catch(ChannelException ex){
							    	throw ex;
							    }
						 }

					    
						counter++;
						if (counter % Constants.POSITION_SAVE_COUNTER == 0) {
							log.debug(
									Constants.POSITION_SAVE_COUNTER
											+ " times file modified checked,save the position Value {} into Disk file",
									positionValue);
							reader.getPositionTracker().save();
							
						}
				}
			}
		}

	}

}
