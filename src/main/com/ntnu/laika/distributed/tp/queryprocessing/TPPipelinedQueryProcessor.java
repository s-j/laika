package com.ntnu.laika.distributed.tp.queryprocessing;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.distributed.tp.TPKernel;
import com.ntnu.laika.distributed.tp.TPQueryBundle;
import com.ntnu.laika.query.processing.ResultHeap;
import com.ntnu.laika.runstats.SimpleStats;
import com.ntnu.laika.structures.LocalIndex;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPPipelinedQueryProcessor implements TPQueryProcessor{
	private DiskInvertedIndex inv;
	private LocalIndex index;
	private Statistics localStats;
	private Statistics globalStats;
//	private ExecutorService workpool;
	private NetworkServer server;
	
	public TPPipelinedQueryProcessor(LocalIndex index, NetworkServer server, ExecutorService workpool) {
//		this.workpool = workpool;
		this.server = server;
		this.index = index;
		localStats = index.getLocalStatistics();
		globalStats = index.getStatistics();
		
		index.loadFastShortDocLengths(globalStats.getNumberOfDocuments());  //access via FastDocLength.getNumberOfTokens(docid)
		index.loadFastShortLexicon(localStats.getNumberOfUniqueTerms());    //access via FastShortLexicon.getLexiconEntry(termId);
		inv = index.getInvertedIndex();										//access via inv.getPostingListIterator(slEntry)
	}

	@Override
	public void processQuery(TPQueryBundle bundle) {
		long time = System.currentTimeMillis();
		
		//if (bundle.getQueryID() >= 5000) SimpleStats.enable();
		//if (bundle.getAccumulatorSet() != null) SimpleStats.addDescription(4, bundle.getAccumulatorSet().size());
		
		TPQueryState state = null;
		switch (TPKernel.processortype){
			case (TPKernel.MSDOR): state = new TPMSDQueryState(bundle, globalStats, inv); break;	
			case (TPKernel.LESTER) : state = new TPLesterQueryState(bundle, globalStats, inv, false, TPKernel.lester_L); break;
			case (TPKernel.SKIPLESTER) : state = new TPLesterQueryState(bundle, globalStats, inv, true, TPKernel.lester_L); break;
			case (TPKernel.AND) : state = new TPANDQueryState(bundle, globalStats, inv); break;
			default : state = new TPORQueryState(bundle, globalStats, inv);
		}
		TPResultState res = state.processQuery();
		time = System.currentTimeMillis() - time;
		state.close();
		
		//System.out.println(myid + " done processing");
		int nextnode = bundle.getNextNodeID();
		bundle.incrementProcessingTime(time);
		
		//if (res.accset != null) SimpleStats.addDescription(5, res.accset.size());
		
		if (nextnode == 0){
			//System.out.println(myid + "done, sending results");
			ChannelBuffer outbuffer = res.resultsToChannelBuffer(bundle);
			res.close();
			server.write(0, outbuffer);
		} else if ((TPKernel.processortype == TPKernel.AND) && res.accset.size() == 0){		//since we do AND processing: no accs -> terminate!
			//System.out.println(myid + " empty intersection, sending results");
			res.cheap = new ResultHeap(0);
			ChannelBuffer outbuffer = res.resultsToChannelBuffer(bundle);
			res.close();
			server.write(0, outbuffer);	
		} else {
			//System.out.println(myid + " sending to next " + nextnode);
			//System.out.println(bundle.getQueryID() +"->" +res.accset.size() +"->"+nextnode);
			ChannelBuffer outbuffer = res.accumulatorsToChannelBuffer(bundle);
			res.close();
			server.write(nextnode, outbuffer);
		}
		//SimpleStats.disable();
	}

	@Override
	public void close() {
		index.close();
	}
}
