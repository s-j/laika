package com.ntnu.network;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public interface MessageHandler {
	public void messageReceived(Channel channel, ChannelBuffer buffer);
}
