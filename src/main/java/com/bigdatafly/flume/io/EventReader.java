/**
 * 
 */
package com.bigdatafly.flume.io;

/**
 * @author summer
 *
 */
public interface EventReader {

	public interface Builer{
		
		public EventReader builder();
	}
}
