package com.bigdatafly.flume.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;

public class PositionTracker implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1705220553574934167L;
	
	private static final Logger log = LoggerFactory
			.getLogger(PositionTracker.class);
	
	
	long position;
	long lastPosition;
	String file;
	File positionTrackerFile;
	
	public PositionTracker(String file){
		
		this(file,0);
	}
	
	public PositionTracker(File file){
		
		this(file,0);
	}
	
	public PositionTracker(String file,long position){
		
		this.file = file;
		this.position = position;
		this.lastPosition = position;
	}
	
	public PositionTracker(File file,long position){
		
		String fileName = "";
		if(file!=null)
			fileName = file.getName();
		this.file = fileName;
		this.position = position;
		this.lastPosition = position;
	}
	
	
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		
		this.lastPosition = this.position;
		this.position = position;
	}
	
	public void mark(long position){
		
		this.lastPosition = this.position;
		this.position = position;
		
		if(this.lastPosition > this.position)
			this.lastPosition = this.position;
	}
	
	
	public void reset(){
		
		this.position = this.lastPosition;
		
	}
	
	private void init0() throws IOException{
		
		String filePath = this.file + File.separator
				+ Constants.POSITION_FILE_NAME;
		positionTrackerFile = new File(filePath);
		if (!positionTrackerFile.exists()) {
			try {
				positionTrackerFile.createNewFile();
				if(log.isDebugEnabled())
					log.debug("Create the position file");
				save();
			} catch (IOException e) {
				log.error("Create the position error", e);
				throw e;
			}
		}
		
	}
	
	public void init(){
		
		InputStream ins = null;
		try{
			init0();
			ins = new FileInputStream(positionTrackerFile);
			List<String> positions = IOUtils.readLines(ins);
			if(positions == null || positions.isEmpty()){
				
				mark(0);
				
			}
				
			String strPosition = positions.get(0);
			this.position = Long.valueOf(strPosition);
		}catch(Exception ex){
			mark(0);
		}finally{
			if(ins != null)
				IOUtils.closeQuietly(ins);
		}
		
	}
	
	public long load(){
		
		InputStream ins = null;
		long position=0;
		try{
			
			ins = new FileInputStream(positionTrackerFile);
			List<String> positions = IOUtils.readLines(ins);
			if(positions == null || positions.isEmpty())
				return 0;
			String strPosition = positions.get(0);
			position = Long.valueOf(strPosition);
		}catch(Exception ex){
			position = 0;
		}finally{
			if(ins != null)
				IOUtils.closeQuietly(ins);
		}
		
		return position;
	}
	
	public void save(){
		
		OutputStream output = null;
		
		try{
			String positionStr = String.valueOf(position);
			output = new FileOutputStream(positionTrackerFile);
			IOUtils.write(positionStr, output);
		}catch(Exception ex){
			if(log.isDebugEnabled())
				log.debug("Write position tracker file failed",ex);
		}finally{
			if(output != null)
				IOUtils.closeQuietly(output);
		}
		
		return ;
	}
	
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}

	@Override
	public String toString() {
		
		return "{file:"+file+",position:"+this.position+",lastPosition:"+this.lastPosition+"}";
	}
	
	
	
	
}
