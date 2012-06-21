package com.ntnu.network;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * A simple synchronization barrier implementation.
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class CntBarrier implements MessageHandler{
	private NetworkServer server;
	private AtomicInteger cnt;
	private byte type;
	
	public CntBarrier(NetworkServer server, byte type, int cnt){
		this.type = type;
		this.cnt = new AtomicInteger(cnt);
		this.server = server;
		server.setMessageHandler(type, this);
	}
	
	public void waitFor(){
		while (cnt.get() > 0){
			synchronized (cnt){
				try {
					cnt.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		server.removeMessageHandler(type);
	}
	
	@Override
	public void messageReceived(Channel channel, ChannelBuffer buffer){
		cnt.decrementAndGet();
		synchronized(cnt){
			cnt.notify();
		}
		//server.removeMessageHandler(type);
	}
}
