package com.ntnu.network;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * This class should handle messages. It takes care of protocol-related
 * messages and forwards any others to a corresponding subscriber. 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */

public class ApplicationHandler extends SimpleChannelHandler { 
	public static final byte HELLO = 0;
	public static final byte HELLO_ACK = 1;
	public static final byte HELLO_FWD = 2;
	
	//public static final byte HOSTS_REQ = 3;
	//public static final byte HOSTS = 4;
	
	
	public static final byte SHUTDOWN = 5;
	public static final byte READY = 6;
	
	public static final byte QUERY = 10;
	public static final byte RESULTS = 11;
	public static final byte QUERY_BUNDLE = 12;
	
	public static final byte DUMMY = -1;
	
	
	private NetworkServer server;
	private HashMap<Byte, MessageHandler> messageHandlers = new HashMap<Byte, MessageHandler>();
	
	ExecutorService external;
	
	public ApplicationHandler(NetworkServer server, ExecutorService external){
		super();
		this.server = server;
		this.external = external;
	}
	
	/*
	 * Called when a channel opens, used to register a new connection.  
	 */
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
		Channel ch = e.getChannel();
		server.registerChannel(ch);
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e){
		ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
		Channel channel = e.getChannel();
		
		byte type = buffer.readByte();
		//System.out.println("got a message - type=" + type + " length=" + buffer.capacity());
		//SimpleStats.addDescription(6, 1, true);
		
		switch (type){
			case (HELLO):
			{
				ConnectionInfo remote = ConnectionMapping.infoFromBuffer(buffer, ((InetSocketAddress)channel.getRemoteAddress()).getAddress().getHostAddress());
				if (server.connectionMapping.hasHostInfo(remote.nodeId)) return;

				//System.out.println(server.connectionMapping.myInfo.nodeId+" hello_accepted"+remote.nodeId);
				//add info
				server.connectionMapping.addHostInfo(remote);
				server.connectionMapping.addChannel(remote.getNodeId(), channel);									
				
				//submit new hello-task
				external.submit(new HelloTask(remote, channel));
			}
			break;
			case (HELLO_FWD):
			{
				ConnectionInfo remote = ConnectionMapping.extendedInfoFromBuffer(buffer);
				if (server.connectionMapping.hasHostInfo(remote.nodeId)) return;
				
				//System.out.println(server.connectionMapping.myInfo.nodeId+" hello_fwd_accepted"+remote.nodeId);
				//submit new hello-fwd task
				external.submit(new HelloFwdTask(remote));
			}
			break;
			case (HELLO_ACK):
			{
				ConnectionInfo remote = ConnectionMapping.infoFromBuffer(buffer, ((InetSocketAddress)channel.getRemoteAddress()).getAddress().getHostAddress());
				if (server.connectionMapping.hasHostInfo(remote.nodeId)) return;
				
				//System.out.println(server.connectionMapping.myInfo.nodeId+" hello_ack_accepted"+remote.nodeId);
				//add info
				server.connectionMapping.addHostInfo(remote);
				server.connectionMapping.addChannel(remote.getNodeId(), channel);
				//server.printKnownHosts();
				
				MessageHandler handler = messageHandlers.get(HELLO);
				if (handler != null) handler.messageReceived(channel, null);
			}
			break;
			default:
				MessageHandler handler = messageHandlers.get(type);
				if (handler != null) handler.messageReceived(channel, buffer);		
		}				
	}
	
	private class HelloFwdTask implements Runnable{
		private ConnectionInfo info;
	
		public HelloFwdTask(ConnectionInfo info){
			this.info = info;
		}
		
		@Override
		public void run(){
			//connect
			Channel ch = server.connect(info.hostName, info.port);
			
			//add info
			server.connectionMapping.addHostInfo(info);
			server.connectionMapping.addChannel(info.getNodeId(), ch);	
			
			//now reply with HELLO_ACK
			ChannelBuffer reply = server.connectionMapping.myInfoToBuffer(true);
			ch.write(reply);
			
			MessageHandler handler = messageHandlers.get(HELLO);
			if (handler != null) handler.messageReceived(ch, null);
		}
	}
	
	private class HelloTask implements Runnable{
		private ConnectionInfo info;
		private Channel ch;
		
		public HelloTask(ConnectionInfo info, Channel ch){
			this.info = info;
			this.ch = ch;
		}
		
		@Override
		public void run(){
			//forward the extended info
			ChannelBuffer msg = ConnectionMapping.extendInfoToBuffer(info);
			server.channels.write(msg);

			//send a hello back
			ChannelBuffer reply = server.connectionMapping.myInfoToBuffer(true);
			ch.write(reply);
			
			MessageHandler handler = messageHandlers.get(HELLO);
			if (handler != null) handler.messageReceived(ch, null);
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e){
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
	
	
	protected void setMessageHandler(byte type, MessageHandler handler){
		messageHandlers.put(type, handler);
	}
	
	protected void removeMessageHandler(byte type){
		messageHandlers.remove(type);
	}
	
	/**	
		ChannelFuture f = ch.write(buffer);
	
		f.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future){
				future.getChannel().close();
				System.out.println("closed!");
			}
		});
	*/
	
}
