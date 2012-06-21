package com.ntnu.laika.distributed.dp.queryprocessing;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.distributed.dp.DPKernel;
import com.ntnu.laika.distributed.dp.DPSubQuery;
import com.ntnu.laika.structures.LocalIndex;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DPQueryProcessor implements Closeable{
	private DiskInvertedIndex inv;
	private LocalIndex index;
	private Statistics localStats;
	private Statistics globalStats;
//	private ExecutorService workpool;
	private NetworkServer server;
	
	public DPQueryProcessor(LocalIndex index, NetworkServer server, ExecutorService workpool) {
//		this.workpool = workpool;
		this.server = server;
		this.index = index;
		localStats = index.getLocalStatistics();
		globalStats = index.getStatistics();
		
		index.loadFastShortDocLengths(globalStats.getNumberOfDocuments());  //access via FastDocLength.getNumberOfTokens(docid)
		index.loadFastShortLexicon(localStats.getNumberOfUniqueTerms());    //access via FastShortLexicon.getLexiconEntry(termId);
		inv = index.getInvertedIndex();										//access via inv.getPostingListIterator(slEntry)
	}

	//@Override
	public void processQuery(DPSubQuery sub) {
		long time = System.currentTimeMillis();
		//System.out.println("got " + sub.getQueryID());
		DPQueryState state = null;
		switch (DPKernel.processortype){
			case (DPKernel.MSDOR): state = new DPMSDQueryState(sub, globalStats, inv); break;	
			case (DPKernel.AND) : state = new DPANDQueryState(sub, globalStats, inv); break;
			default : state = new DPORQueryState(sub, globalStats, inv);
		}
		DPResultState res = state.processQuery();
		time = System.currentTimeMillis() - time;
		state.close();
		
		//System.out.println(myid + " done processing");
		sub.incrementProcessingTime(time);
		
		//System.out.println(myid + "done, sending results");
		ChannelBuffer outbuffer = res.resultsToChannelBuffer(sub);
		
		res.close();
		//System.out.println("ret " + sub.getQueryID());
		server.write(0, outbuffer);
	}

	@Override
	public void close() {
		index.close();
	}
}
