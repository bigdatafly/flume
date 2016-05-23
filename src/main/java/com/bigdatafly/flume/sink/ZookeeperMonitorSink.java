/**
 * 
 */
package com.bigdatafly.flume.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.flume.common.Constants;
import com.bigdatafly.flume.log.LogEvent;
import com.bigdatafly.flume.utils.JsonUtils;
import com.bigdatafly.flume.zookeeper.NodeLog;
import com.bigdatafly.flume.zookeeper.Zookeeper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


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
	//private String hostName ;
	//private String currentNode;
	private String parentNode;
	private Long   updateInterval;
	private Long   lastUpdateTime = 0L;
	private boolean zkIsInit = false;
	private final static long DEFAULT_UPDATE_INTERVAL = 10;
	private Map<String,NodeLog> monitorMap = new HashMap<String,NodeLog>();
	private SinkCounter sinkCounter;
	@Override
	public synchronized void start() {
		
		
		try {
			if(zk == null)
				zk = zookeeper.mkClient(null,Arrays.asList(zkServers), Constants.ZOOKEEPER_PORT, "");
			if(!zookeeper.existsNode(zk, Constants.ZOOKEEPER_FLUME_NODE, true)){
				zookeeper.createNode(zk, Constants.ZOOKEEPER_FLUME_NODE, Constants.ZOOKEEPER_FLUME_NODE.getBytes());
			}
			
			//dataFlow = getFlowCount();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		sinkCounter.start();
		super.start();
		
		log.debug("{} started",getName());
	}

	@Override
	public synchronized void stop() {
		
		if(zk!=null)
			zk.close();
		sinkCounter.stop();
		log.info("Kafka Sink {} stopped. Metrics: {}", getName(), sinkCounter);
		super.stop();
	}


    private int getBatchSize(){
    	
    	return 500;
    }
	
	public Status process() throws EventDeliveryException {
		
		Status status = Status.READY;
		Long currentDataTime = System.currentTimeMillis();
		
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		String hostname ;
		try{
			transaction.begin();
			List<Event> batch = Lists.newLinkedList();
			for (int i = 0; i < getBatchSize(); i++) {
		        event = channel.take();
		        if (event == null) {
		          break;
		        }

		        batch.add(event);
		    }
			
			int size = batch.size();
			int batchSize = getBatchSize();
			
			if(size == 0){
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
				
			}else{
				
				if (size < batchSize) {
			          sinkCounter.incrementBatchUnderflowCount();
			    } else {
			          sinkCounter.incrementBatchCompleteCount();
			    }
			    sinkCounter.addToEventDrainAttemptCount(size);
			    
				for(int i=0;i<size;i++){
					
					Event event1 = batch.get(i);
					if(log.isDebugEnabled()){
						log.debug("Event:" + EventHelper.dumpEvent(event1));
					}
					
					LogEvent log = serialize(event1);
					if(log == null)
						continue;
					hostname = getHostnameFromEvent(event1.getHeaders(),log);
					NodeLog nodeLog = getHostData(hostname);
					if(nodeLog == null)
						continue;
					nodeLog.setFlow(nodeLog.getFlow() + log.getLen());
					
				}
				
				if(currentDataTime - lastUpdateTime > updateInterval){
					
					for(Map.Entry<String, NodeLog> e : this.monitorMap.entrySet()){
						setFlowCountOnZookeeper( zookeeper, zk, e.getValue());
					}
					
					lastUpdateTime = currentDataTime;
				}
			    
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
		/*
		hostName = OSUtils.getHostName(OSUtils.getInetAddress());
		Preconditions.checkArgument(hostName != null,
				"the local host name can not be null !");
		*/
		//currentNode = Constants.ZOOKEEPER_FLUME_NODE + "/" + hostName;
		parentNode = Constants.ZOOKEEPER_FLUME_NODE;
		
		long interval = context.getLong(Constants.UPDATE_INTERVAL_KEY,DEFAULT_UPDATE_INTERVAL);
		
		if(interval<1)
			interval = DEFAULT_UPDATE_INTERVAL;
		updateInterval = interval*1000;
		
		lastUpdateTime = System.currentTimeMillis();
		
		if(sinkCounter == null)
			 sinkCounter = new SinkCounter(getName());
	}

	private String getHostnameFromEvent(Map<String,String> headers,LogEvent log){

		String hostname="";
		if(headers != null && headers.containsKey(Constants.HOST_NAME_HEADER)){
			hostname = headers.get(Constants.HOST_NAME_HEADER);
		}
		if(log!=null && StringUtils.isEmpty(hostname))
			hostname = log.getIp();
		
		return hostname;
	}
	
	private LogEvent serialize(Event event){

		byte[] body = event.getBody();
		if(body == null)
			return null;
		LogEvent log = JsonUtils.fromJson(new String(body), LogEvent.class);
		return log;
	}
	
	private NodeLog getHostData(String hostName){
		
		if(StringUtils.isEmpty(hostName))
			return null;
		
		if(monitorMap.containsKey(hostName))
			return monitorMap.get(hostName);
		else{
			NodeLog nodeLog= getHostDataFromZK(hostName);
			if(nodeLog !=null)
				monitorMap.put(hostName, nodeLog);
			return nodeLog;
		}
	
		
	}
	
	private NodeLog getHostDataFromZK(String hostName) {
		
		NodeLog nodeLog = null;
		try{
			String path = parentNode + "/" + hostName;
			if(!zookeeper.existsNode(zk, path, true)){
				//zookeeper.createNode(zk, path, JsonUtils.toJson(new NodeLog(hostName)).getBytes(),CreateMode.EPHEMERAL);
				return null;
				
			}else{
				byte[] byteLog = zookeeper.getData(zk, path, false);
				
				if(byteLog !=null){
					String logJson = new String(byteLog);
					nodeLog = JsonUtils.fromJson(logJson, NodeLog.class);
				}
			}
		}catch(Exception ex){
			nodeLog = null;
		}
		
		return nodeLog;
		
	}
	
	
	public void setFlowCountOnZookeeper(Zookeeper zookeeper,CuratorFramework zk,NodeLog nodeLog) throws Exception{
		
		String targetHostname = nodeLog.getHost();
		String path = parentNode + "/" + targetHostname;
		
		try {
			if(!zkIsInit){
				if(!zookeeper.existsNode(zk, path, true)){
					zookeeper.createNode(zk, path, JsonUtils.toJson(new NodeLog(targetHostname)).getBytes(),CreateMode.EPHEMERAL);
				}
				zkIsInit = true;
			}
		
			zookeeper.setData(zk, path, JsonUtils.toJson(nodeLog).getBytes());
		} catch (Exception e) {
			log.error("save data into Zookeeper Error", e);
			throw e;
		}
		
		
	}
}
