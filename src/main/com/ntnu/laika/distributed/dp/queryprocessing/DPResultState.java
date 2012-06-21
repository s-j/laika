package com.ntnu.laika.distributed.dp.queryprocessing;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.distributed.dp.DPSubQuery;
import com.ntnu.laika.distributed.util.RemoteQueryResults;
import com.ntnu.laika.query.processing.ResultHeap;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DPResultState implements Closeable {
	protected ResultHeap cheap;
	protected int numberOfResults;
	
	public DPResultState(ResultHeap cheap, int numberOfResults){
		this.cheap = cheap;
		this.numberOfResults = numberOfResults;
	}
	
	public final ChannelBuffer resultsToChannelBuffer(DPSubQuery sub){
		return RemoteQueryResults.toChannelBuffer(sub.getQueryID(), cheap, numberOfResults, sub.getProcessingTime());
	}
	
	@Override
	public void close(){}
}