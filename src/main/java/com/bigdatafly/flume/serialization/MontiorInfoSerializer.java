/**
 * 
 */
package com.bigdatafly.flume.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.serialization.BodyTextEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;

/**
 * @author summer
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MontiorInfoSerializer implements EventSerializer {

	private final static Logger logger =
		      LoggerFactory.getLogger(MontiorInfoSerializer.class);


	private final OutputStream out;
	private final String flowCountHeader;
	
	private MontiorInfoSerializer(OutputStream out, Context ctx) {
		
	    this.out = out;
	    this.flowCountHeader = ctx.getString(Constants.FLOW_COUNT_HEADER,
	    		Constants.FLOW_COUNT_HEADER);
	  }
	
	public void afterCreate() throws IOException {
		
		// noop
	}

	public void afterReopen() throws IOException {
		// noop
		
	}

	public void write(Event event) throws IOException {
		
		Map<String,String> headers = event.getHeaders();
		long flow = 0;
		if(headers.containsKey(flowCountHeader)){
			String strFlow = headers.get(flowCountHeader);
			try{
				flow = Long.parseLong(strFlow);
			}catch(NumberFormatException ex){}
		}
		
		//out.w(flow);
		//NodeLog nodeLog = new NodeLog();
		//nodeLog.setFlow(flow);
		//setFlowCountOnZookeeper(Constants.ZOOKEEPER_FLUME_NODE,nodeLog);
		
	}

	public void flush() throws IOException {
		// noop
		
	}

	public void beforeClose() throws IOException {
		// noop
		
	}

	public boolean supportsReopen() {
		
		return true;
	}

	public static class Builder implements EventSerializer.Builder {

	    public EventSerializer build(Context context, OutputStream out) {
	    	
	    	MontiorInfoSerializer s = new MontiorInfoSerializer(out, context);
	    	return s;
	    }

	  }

}
