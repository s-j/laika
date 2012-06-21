package com.ntnu.network;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

/**
 * Represents topology (node/id) to hostname/protnumber mapping. 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class ConnectionMapping {
	private static final int INFOBYTES_LENGTH = 24;
	private static final int HOSTNAME_LENGTH = 21;
	private static final byte[] noHostNameBuffer = new byte[HOSTNAME_LENGTH];
	
	private ConcurrentHashMap<Integer, ConnectionInfo> knownHosts;
	private ConcurrentHashMap<Integer, Channel> openChannels;
	private ConcurrentHashMap<Channel, Integer> reverseChannels;
	
	protected final ConnectionInfo myInfo;
	
	public ConnectionMapping(ConnectionInfo myInfo){
		this.myInfo = myInfo;
		knownHosts = new ConcurrentHashMap<Integer, ConnectionInfo>();
		openChannels = new ConcurrentHashMap<Integer, Channel>();
		reverseChannels = new ConcurrentHashMap<Channel, Integer>();
	}
	
	public final Channel getChannel(int nodeId){
		return openChannels.get(nodeId);
	}

	public void addChannel(int nodeId, Channel ch){
		openChannels.put(nodeId, ch);
		reverseChannels.put(ch, nodeId);
	}
	
	private void removeChannel(int nodeId, Channel ch){
		openChannels.remove(nodeId);
		reverseChannels.remove(ch);
	}
	
	public void removeChannel(int nodeId){
		removeChannel(nodeId, openChannels.get(nodeId));
	}
	
	public void removeChannel(Channel ch){
		removeChannel(reverseChannels.get(ch), ch);
	}
	
	public ConnectionInfo getHostInfo(int nodeId){
		return knownHosts.get(nodeId);
	}
	
	public void addHostInfo(ConnectionInfo info){
		knownHosts.put(info.nodeId, info);
	}
	
	public boolean hasHostInfo(Integer nodeId){
		if (nodeId == myInfo.nodeId) return true;
		return knownHosts.containsKey(nodeId);
	}
	
	public void removeKnownHost(int nodeId){
		knownHosts.remove(nodeId);
	}
	
	public ChannelBuffer myInfoToBuffer(boolean ack){
		ChannelBuffer buffer = ChannelBuffers.buffer(4);
		buffer.writeByte(ack?ApplicationHandler.HELLO_ACK:ApplicationHandler.HELLO);
		
		buffer.writeByte(0xff & (myInfo.nodeId));
		buffer.writeByte(0xff & (myInfo.port >>> 8));
		buffer.writeByte(0xff & (myInfo.port));
		return buffer;
	}
	
	public static ConnectionInfo infoFromBuffer(ChannelBuffer buffer, String hostName){
		int nodeId = buffer.readByte();
		int port = buffer.readByte() << 8;
			port |= buffer.readByte();
		return new ConnectionInfo(nodeId, hostName, port);
	}
	
	public static ChannelBuffer extendInfoToBuffer(ConnectionInfo info){
		ChannelBuffer newBuffer = ChannelBuffers.buffer(INFOBYTES_LENGTH+1);
		newBuffer.writeByte(ApplicationHandler.HELLO_FWD);
		newBuffer.writeByte(0xff & (info.nodeId));
		newBuffer.writeByte(0xff & (info.port >>> 8));
		newBuffer.writeByte(0xff & (info.port));
		byte[] _hostName = info.hostName.getBytes();
		newBuffer.writeBytes(_hostName);
		newBuffer.writeBytes(noHostNameBuffer, 0, HOSTNAME_LENGTH - _hostName.length);
		return newBuffer;
	}
	
	public static ConnectionInfo extendedInfoFromBuffer(ChannelBuffer buffer){
		int nodeId = buffer.readByte();
		int port = buffer.readByte() << 8;
			port |= buffer.readByte();

		byte[] _hostName = new byte[HOSTNAME_LENGTH];
		buffer.readBytes(_hostName);
		return new ConnectionInfo(nodeId, (new String(_hostName)).trim(), port);
	}
		
	public void printKnownHosts(){
		Collection<ConnectionInfo> hosts = knownHosts.values();
		System.out.println(myInfo.nodeId+"..size " + hosts.size());
		for (ConnectionInfo info : hosts){
			System.out.println(myInfo.nodeId+".. "+info.nodeId+" "+info.port);
		}
	}
	
	/*
	public ChannelBuffer knownHostsToBuffer(){
		Collection<ConnectionInfo> hosts = knownHosts.values();
		
		int num = hosts.size();
		ChannelBuffer buffer = ChannelBuffers.buffer(num*INFOBYTES_LENGTH+1);
		buffer.writeByte(ApplicationHandler.HOSTS);
		
		byte[] _hostName;
		for (ConnectionInfo info : hosts){
			buffer.writeByte((byte)(0xff & (info.nodeId)));
			buffer.writeByte((byte)(0xff & (info.port >>> 8)));
			buffer.writeByte((byte)(0xff & (info.port)));
			
			_hostName = info.hostName.getBytes();
			buffer.writeBytes(_hostName);
			buffer.writeBytes(noHostNameBuffer, 0, HOSTNAME_LENGTH - _hostName.length);
		}
		
		return buffer;
	}
	
	public static ConnectionInfo[] infosFromBuffer(ChannelBuffer buffer){
		int num = buffer.readableBytes() / INFOBYTES_LENGTH;
		ConnectionInfo[] infos = new ConnectionInfo[num];
		int _hostId, _portNr;
		byte[] _hostName = new byte[HOSTNAME_LENGTH];

		for (int i=0; i<num; i++){
			_hostId = buffer.readByte();
			_portNr = buffer.readByte() << 8;
			_portNr |= buffer.readByte();
			buffer.readBytes(_hostName);
			infos[i] = new ConnectionInfo(_hostId, (new String(_hostName)).trim(), _portNr); 
		}
		
		return infos;
	}
	*/
}
