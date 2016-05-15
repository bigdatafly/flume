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
	
	@Override
	public synchronized void start() {
		
		try {
			if(zk == null)
				zk = zookeeper.mkClient(null,Arrays.asList(zkServers), Constants.ZOOKEEPER_PORT, "");
			if(!zookeeper.existsNode(zk, Constants.ZOOKEEPER_FLUME_NODE, true)){
				zookeeper.createNode(zk, Constants.ZOOKEEPER_FLUME_NODE, Constants.ZOOKEEPER_FLUME_NODE.getBytes());
			}
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

	@Override
	public Status process() throws EventDeliveryException {
		
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		try{
			transaction.begin();
			event = channel.take();
			if(event !=null){
				
				if(log.isInfoEnabled()){
					log.info("Event:" + EventHelper.dumpEvent(event));
				}
				
				/*
				Map<String,String> headers = event.getHeaders();
				long flow = 0;
				if(headers.containsKey(Constants.FLOW_COUNT_HEADER)){
					String strFlow = headers.get(Constants.FLOW_COUNT_HEADER);
					try{
						flow = Long.parseLong(strFlow);
					}catch(NumberFormatException ex){}
				}
				NodeLog nodeLog = new NodeLog();
				nodeLog.setFlow(flow);
				setFlowCountOnZookeeper(Constants.ZOOKEEPER_FLUME_NODE,nodeLog);
				*/
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

	@Override
	public void configure(Context context) {
		
		String zkConf = context.getString(Constants.ZOOKEEPER_CLUSTER_KEY,"VM-G101-07-73,VM-G101-07-74,VM-G101-07-75");
		
		log.debug("zkConf:" + zkConf );
		zkServers = zkConf.split(",");
		Preconditions.checkArgument(zkServers != null,
				"the zookeeper servers parameters can not be null !");
	}

	public void setFlowCountOnZookeeper(String parent, NodeLog nodeLog) throws Exception{
		
		String hostname =  OSUtils.getHostName(OSUtils.getInetAddress());
		String path = parent + "/" + hostname;
		
		//NodeLog nodeLog = new NodeLog();
		//nodeLog.setFlow(data);
		nodeLog.setHost(hostname);
		long flow = 0;
		try {
			if(!zookeeper.existsNode(zk, path, true)){
				zookeeper.createNode(zk, path, "node-".concat(hostname).getBytes(),CreateMode.EPHEMERAL);
			}
			byte[] byteLog = zookeeper.getData(zk, path, true);
			
			if(byteLog !=null){
				String logJson = new String(byteLog);
				NodeLog log = JsonUtils.fromJson(logJson, NodeLog.class);
				if(log!=null)
					flow = log.getFlow();
				
			}
			
			nodeLog.setFlow(nodeLog.getFlow()+flow);
			zookeeper.setData(zk, path, JsonUtils.toJson(nodeLog).getBytes());
		} catch (Exception e) {
			log.error("save data into Zookeeper Error", e);
			throw e;
		}
		
		
	}
	
	public MontiorInfo
}
