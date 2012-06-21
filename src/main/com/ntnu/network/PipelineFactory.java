package com.ntnu.network;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.logging.LoggingHandler;


/**
 * A custom channel pipeline factory implementation, consisting of logHandler,
 * LengthFieldPrepender/LFBasedFrameDecored and the ApplicationHandler. 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class PipelineFactory implements ChannelPipelineFactory{
	private LoggingHandler logHandler;
	private ApplicationHandler appHandler;
	
	public PipelineFactory(NetworkServer server, ApplicationHandler appHandler){
		logHandler = new LoggingHandler();
		this.appHandler = appHandler;
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();
		
		pipeline.addLast("logger", logHandler);
		pipeline.addLast("framer", new LengthFieldPrepender(4));
		pipeline.addLast("reframer", new LengthFieldBasedFrameDecoder(2147483647, 0, 4, 0, 4));
		pipeline.addLast("app", appHandler);

		return pipeline;
	}
	
}
