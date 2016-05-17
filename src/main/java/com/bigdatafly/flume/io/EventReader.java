/**
 * 
 */
package com.bigdatafly.flume.io;

import java.util.List;

import org.apache.flume.Event;

/**
 * @author summer
 *
 */
public interface EventReader {

	public List<Event> readEvents();
	
	public PositionTracker getPositionTracker();
	
	public static interface Builer{
		
		public EventReader builder();
	}
}
