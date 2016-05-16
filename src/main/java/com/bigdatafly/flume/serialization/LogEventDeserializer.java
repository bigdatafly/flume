/**
 * 
 */
package com.bigdatafly.flume.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventHelper;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;
import com.google.common.base.Preconditions;

/**
 * @author summer
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LogEventDeserializer implements EventDeserializer {

	private final static Logger logger =
		      LoggerFactory.getLogger(LogEventDeserializer.class);


	private final OutputStream out;
	private final String flowCountHeader;
	private final String hostNameHeader;
	
	private LogEventDeserializer(OutputStream out, Context ctx) {
		
	    this.out = out;
	    this.flowCountHeader = ctx.getString(Constants.FLOW_COUNT_HEADER,
	    		Constants.FLOW_COUNT_HEADER);
	    this.hostNameHeader = ctx.getString(Constants.HOST_NAME_HEADER,Constants.HOST_NAME_HEADER);
	    
	    Preconditions.checkArgument(this.out!=null, "out must not be null!");
	    Preconditions.checkArgument(this.flowCountHeader!=null,"flow count header must not be null.");
	    Preconditions.checkNotNull(this.hostNameHeader,"host name header must not be null");
	  }
	


	public static class Builder implements EventDeserializer.Builder {

		public EventDeserializer build(Context context, ResettableInputStream in) {
			
			return null;
		}

	  }



	public Event readEvent() throws IOException {
		
		return readEvents(1);
	}



	public List<Event> readEvents(int numEvents) throws IOException {
		
		return null;
	}



	public void mark() throws IOException {
		
		
	}



	public void reset() throws IOException {
		
		
	}



	public void close() throws IOException {
		
		
	}

}
