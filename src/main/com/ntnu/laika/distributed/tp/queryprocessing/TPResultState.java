package com.ntnu.laika.distributed.tp.queryprocessing;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.distributed.tp.TPQueryBundle;
import com.ntnu.laika.distributed.util.AccumulatorSet;
import com.ntnu.laika.distributed.util.RemoteQueryResults;
import com.ntnu.laika.query.processing.ResultHeap;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPResultState implements Closeable {
	protected AccumulatorSet accset;
	protected ResultHeap cheap;
	protected double minScore;
	protected double maxScore;
	protected double kthBest;
	protected int numberOfResults;
	
	public TPResultState(AccumulatorSet accset, ResultHeap cheap, double minScore, double maxScore, double kthBest, int numberOfResults){
		this.accset = accset;
		this.cheap = cheap;
		this.minScore = minScore;
		this.maxScore = maxScore;
		this.kthBest = kthBest;
		this.numberOfResults = numberOfResults;
	}
	
	public final ChannelBuffer resultsToChannelBuffer(TPQueryBundle bundle){
		return RemoteQueryResults.toChannelBuffer(bundle.getQueryID(), cheap, numberOfResults, bundle.getProcessingTime());
	}
	
	public final ChannelBuffer accumulatorsToChannelBuffer(TPQueryBundle bundle){
		return bundle.packNextQueryBundle(accset, minScore, maxScore, kthBest);
	}
	
	@Override
	public void close() {
		if (accset != null) accset.close();
	}
	
	
}