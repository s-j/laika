package com.ntnu.network;

/**
 * A simple description of a connection to be used to identify peers and connections.
 * @author  <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class ConnectionInfo{
	protected int nodeId;
	protected String hostName;
	protected int port;
	
	
	public ConnectionInfo(int nodeId, String hostName, int port){
		this.nodeId = nodeId;
		this.port = port;
		this.hostName = hostName;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public int getPort(){
		return port;
	}
	
	public int getNodeId(){
		return nodeId;
	}
	
	public String toString(){
		return nodeId + "-" + hostName + ":" + port;
	}
	
	@Override
	public int hashCode(){
		return nodeId;
	}
	
}
