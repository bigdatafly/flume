/**
 * 
 */
package com.bigdatafly.flume.sink;

import java.util.Arrays;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.utils.JsonUtils;
import com.bigdatafly.flume.utils.OSUtils;
import com.bigdatafly.flume.zookeeper.NodeLog;
import com.bigdatafly.flume.zookeeper.Zookeeper;
import com.google.common.base.Preconditions;


/**
 * @author summer
 *
 */
public class ZookeeperMonitorSink extends AbstractSink implements Configurable{

	private static final Logger log = LoggerFactory
			.getLogger(ZookeeperMonitorSink.class);
	
	private Zookeeper zookeeper = new Zookeeper();
	private CuratorFramework zk;
	private String[] zkServers;
	private long dataFlow = 0L ;
	private String hostName ;
	private String currentNode;
	private Long   updateInterval;
	private Long   lastUpdateTime = 0L;
	private final static long DEFAULT_UPDATE_INTERVAL = 10;
	
	@Override
	public synchronized void start() {
		
		
		try {
			if(zk == null)
				zk = zookeeper.mkClient(null,Arrays.asList(zkServers), Constants.ZOOKEEPER_PORT, "");
			if(!zookeeper.existsNode(zk, Constants.ZOOKEEPER_FLUME_NODE, true)){
				zookeeper.createNode(zk, Constants.ZOOKEEPER_FLUME_NODE, Constants.ZOOKEEPER_FLUME_NODE.getBytes());
			}
			
			dataFlow = getFlowCount();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
		super.start();
		
		log.debug("{} started",getName());
	}

	@Override
	public synchronized void stop() {
		
		if(zk!=null)
			zk.close();
		super.stop();
	}


	public Status process() throws EventDeliveryException {
		
		Status status = Status.READY;
		Long currentDataTime = System.currentTimeMillis();
		
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		try{
			transaction.begin();
			event = channel.take();
			if(event !=null){
				
				if(log.isDebugEnabled()){
					log.debug("Event:" + EventHelper.dumpEvent(event));
				}
				
				Map<String,String> headers = event.getHeaders();
				long flow = 0;
				if(headers.containsKey(Constants.FLOW_COUNT_HEADER)){
					String strFlow = headers.get(Constants.FLOW_COUNT_HEADER);
					try{
						flow = Long.parseLong(strFlow);
					}catch(NumberFormatException ex){}
				}
				
				dataFlow += flow;
			    
				if(currentDataTime - lastUpdateTime > updateInterval){
					
					setFlowCountOnZookeeper(zookeeper,zk,dataFlow);
					lastUpdateTime = currentDataTime;
				}
				
				//setFlowCountOnZookeeper(zookeeper,zk,dataFlow);
				
			}else{
				status = Status.BACKOFF;
			}
			transaction.commit();
		}catch(Exception ex){
			transaction.rollback();
			throw new EventDeliveryException("Failed to monitor file event",ex);
		}finally{
			transaction.close();
		}
		
		
		return status;
	}


	public void configure(Context context) {
		
		String zkConf = context.getString(Constants.ZOOKEEPER_CLUSTER_KEY,"localhost");
		
		if(log.isDebugEnabled())
			log.debug("zkConf:" + zkConf );
		zkServers = zkConf.split(",");
		Preconditions.checkArgument(zkServers != null,
				"the zookeeper servers parameters can not be null !");
		
		hostName = OSUtils.getHostName(OSUtils.getInetAddress());
		Preconditions.checkArgument(hostName != null,
				"the local host name can not be null !");
		
		currentNode = Constants.ZOOKEEPER_FLUME_NODE + "/" + hostName;
		
		
		long interval = context.getLong(Constants.UPDATE_INTERVAL_KEY,DEFAULT_UPDATE_INTERVAL);
		
		if(interval<1)
			interval = DEFAULT_UPDATE_INTERVAL;
		updateInterval = interval*1000;
		
		lastUpdateTime = System.currentTimeMillis();
	}

	private long getFlowCount() throws Exception{
		
		long dataFlow = 0L;
		String path = currentNode;
		if(!zookeeper.existsNode(zk, path, true)){
			
			zookeeper.createNode(zk, path, JsonUtils.toJson(new NodeLog(hostName)).getBytes(),CreateMode.EPHEMERAL);
			
		}else{
			byte[] byteLog = zookeeper.getData(zk, path, true);
			
			if(byteLog !=null){
				String logJson = new String(byteLog);
				NodeLog log = JsonUtils.fromJson(logJson, NodeLog.class);
				if(log!=null)
					dataFlow = log.getFlow();
				
			}
		}
		
		return dataFlow;
		
	}
	
	public void setFlowCountOnZookeeper(Zookeeper zookeeper,CuratorFramework zk,long dataFlow) throws Exception{
		
		String path = currentNode;
		
		try {
			
			long flow =  dataFlow ;
			NodeLog nodeLog = new NodeLog();
			nodeLog.setHost(hostName);
			nodeLog.setFlow(flow);
			zookeeper.setData(zk, path, JsonUtils.toJson(nodeLog).getBytes());
		} catch (Exception e) {
			log.error("save data into Zookeeper Error", e);
			throw e;
		}
		
		
	}
}
