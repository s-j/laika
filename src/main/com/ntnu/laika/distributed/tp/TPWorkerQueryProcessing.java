package com.ntnu.laika.distributed.tp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import com.ntnu.laika.Constants;
import com.ntnu.laika.distributed.tp.queryprocessing.TPPipelinedQueryProcessor;
import com.ntnu.laika.distributed.tp.queryprocessing.TPQueryProcessor;
import com.ntnu.laika.structures.LocalIndex;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.network.MessageHandler;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPWorkerQueryProcessing  implements Closeable, MessageHandler{
	protected TPKernel kernel;
	protected LocalIndex index;
	protected NetworkServer server;
	protected volatile boolean terminated;
	protected int myid;
	protected TPQueryProcessor processor;
	
	public TPWorkerQueryProcessing(TPKernel kernel){
		this.kernel = kernel;
		this.server = kernel.server;
		this.index = (LocalIndex) kernel.index;
		this.myid = kernel.nodeId;
		
		Constants.USE_SKIPS = index.getIndexProperties().getProperty("use_skips").equals("true")?true:false;
			
		processor = new TPPipelinedQueryProcessor(index, server, kernel.workpool);
		terminated = false;
	}
	
	private class QueryTask implements Runnable{
		private ChannelBuffer buffer;
		
		public QueryTask(ChannelBuffer buffer){
			this.buffer = buffer;
		}
		
		@Override
		public void run() {
			try {
				TPQueryBundle qb = new TPQueryBundle(buffer, myid);
				processor.processQuery(qb);	
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
			
		}
	}
	
	@Override
	public void messageReceived(Channel channel, ChannelBuffer buffer) {
		//System.out.println("received a query");
		if (!terminated){
			kernel.workpool.submit(new QueryTask(buffer));
		}
	}
	
	public void shutdown(){
		terminated = true;
	}

	@Override
	public void close() {
		processor.close();
	}
}
