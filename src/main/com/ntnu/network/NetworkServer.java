package com.ntnu.network;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * The main class for the network server, encapsulates main controls over Netty.
 * Provides methods to open new and close existing network connections.
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */

public class NetworkServer {
	
	private ChannelFactory serverFactory;
	private ChannelFactory clientFactory;
	private ServerBootstrap serverBootstrap;
	private ClientBootstrap clientBootstrap;
	private ApplicationHandler appHandler;
	private PipelineFactory pipelineFactory;
	private Channel acceptor;
	
	protected ChannelGroup channels;
	protected ConnectionMapping connectionMapping;
	private int nodeId;
	
	public NetworkServer(int nodeId, int portNr, ExecutorService external){
		this.nodeId = nodeId;
		connectionMapping = new ConnectionMapping(new ConnectionInfo(nodeId, null, portNr));
		
		appHandler = new ApplicationHandler(this, external);
		pipelineFactory = new PipelineFactory(this, appHandler);
		channels =  new DefaultChannelGroup("-all-channels");

		ExecutorService boss = Executors.newCachedThreadPool();
		ExecutorService worker = Executors.newCachedThreadPool();
		serverFactory = new NioServerSocketChannelFactory(boss, worker);
		serverBootstrap = new ServerBootstrap(serverFactory);	
		serverBootstrap.setPipelineFactory(pipelineFactory);
		serverBootstrap.setOption("child.tcpNoDelay", true);
		serverBootstrap.setOption("child.keepAlive", true);		
		acceptor = serverBootstrap.bind(new InetSocketAddress(portNr));
        //channels.add(acceptor);
        
		clientFactory = new NioClientSocketChannelFactory(boss, worker);
		clientBootstrap = new ClientBootstrap(clientFactory);
		clientBootstrap.setPipelineFactory(pipelineFactory);
		clientBootstrap.setOption("tcpNoDelay", true);
		clientBootstrap.setOption("keepAlive", true);
	}

	/**
	 * adds a registered channel to the all-channels group
	 * @param ch
	 */
	public void registerChannel(Channel ch){
		//System.out.println(nodeId+" registered " + ch.getLocalAddress()+ " " + ch.getRemoteAddress());
		channels.add(ch);
	}
	
	/**
	 * closes a given channel and remove it from the all-channels group
	 * @param ch
	 */
	public void closeChannel(Channel ch){
		channels.remove(ch);
		ch.close();
	}
	
	/**
	 * debug, prints the total number of open channels.
	 */
	public void printChannels(){
		System.out.println("Channels: " + channels.size());
		Iterator<Channel> iter = channels.iterator();
		while(iter.hasNext()){
			Channel ch = iter.next();
			System.out.println(">>" + nodeId+ " "+ ch.getLocalAddress()+" "+ch.getRemoteAddress());
		}
	}
	
	public void printKnownHosts(){
		connectionMapping.printKnownHosts();
	}
	
	/**
	 * shuts down by closing acceptor, all-channels and then releases the external resources. 
	 */
	public void shutdown(){
		System.out.println("shutting down!");
		ChannelFuture future_ = acceptor.close();
		future_.awaitUninterruptibly();
		ChannelGroupFuture future = channels.close();
		future.awaitUninterruptibly();
		System.out.println("shuted down");
		//serverFactory.releaseExternalResources();
		//clientFactory.releaseExternalResources();
		System.out.println("released resources");
		System.exit(0);
	}
	
	/**
	 * sends a SHUTDOWN request to all-channels and then shuts down the server itself.
	 */
	public void shutdownAll(){
		System.out.println("sending shutdown_all");
		ChannelBuffer buffer = ChannelBuffers.buffer(1);
		buffer.writeByte(ApplicationHandler.SHUTDOWN);
		ChannelGroupFuture future = channels.write(buffer);
		future.awaitUninterruptibly();
		System.out.println("shutdown_all sent");
		shutdown();
	}
	
	/**
	 * writes a ChannelBuffer to a Channel
	 * @param ch the Channel to write to
	 * @param buffer a particular CahnnelBuffer to write
	 * @return
	 */
	public ChannelFuture write(Channel ch, ChannelBuffer buffer){
		//SimpleStats.addDescription(5, 1, true);
		return ch.write(buffer);
	}
	
	public ChannelFuture write(int nodeid, ChannelBuffer buffer){
		//System.out.println("sending to " + nodeid);
		//SimpleStats.addDescription(5, 1, true);
		ChannelFuture ft = connectionMapping.getChannel(nodeid).write(buffer);
		//System.out.println("sending to " + nodeid);
		return ft;
	}
	
	/*
	 * Forces this node to connect to any host known by the remoteHost given by ch
	 * @param ch channel to a remote host
	
	public ChannelFuture explore(Channel ch){
		//initiate hostInfo exchange!
		ChannelBuffer buffer = ChannelBuffers.buffer(1);
		buffer.writeByte(ApplicationHandler.HOSTS_REQ);
		return ch.write(buffer);
	}*/
	
	/**
	 * connects the server to a remote host specified by InetSocketAddress
	 * @param addr remote host address
	 * @return
	 */
	public Channel connect(InetSocketAddress addr){
		ChannelFuture f = clientBootstrap.connect(addr);
		f.awaitUninterruptibly();
		Channel ch = f.getChannel();
		
		//System.out.println("pip!" + ch.getRemoteAddress());
		return ch;
	}
	
	public void sayHello(Channel ch){
		ChannelBuffer buffer = connectionMapping.myInfoToBuffer(false);
		ChannelFuture f = ch.write(buffer);
		f.awaitUninterruptibly();
	}
	
	/**
	 * connects to a remote host specified by a hostname and a portnumber
	 * @param host
	 * @param port
	 * @return
	 */
	public Channel connect(String host, int port){
		return connect(new InetSocketAddress(host, port));
	}

	public void setMessageHandler(byte type, MessageHandler handler){
		appHandler.setMessageHandler(type, handler);
	}
	
	public void removeMessageHandler(byte type){
		appHandler.removeMessageHandler(type);
	}

}
