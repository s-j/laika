package com.ntnu.laika.distributed.dp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import com.ntnu.laika.Constants;
import com.ntnu.laika.distributed.dp.DPMasterQueryPreprocessing.MasterQuery;
import com.ntnu.laika.distributed.util.RemoteQueryResults;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.QueryResultsIterator;
import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconEntry;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.laika.utils.Pair;
import com.ntnu.laika.utils.Triple;
import com.ntnu.network.MessageHandler;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DPQueryDispatcher implements Runnable, Closeable, MessageHandler{
	
	private BufferedReader querylogReader;
	private DPMasterQueryPreprocessing preproc;
	
	private DPKernel kernel;
	private MasterIndex index;
	private NetworkServer server;
	
	private ArrayBlockingQueue<Pair<Integer, ChannelBuffer[]>> outBuffer;
	private ArrayBlockingQueue<ChannelBuffer> inBuffer;
	private int remcnt, remslots, sent;
	
	private int pre, stopAfter;
	private volatile boolean terminated = false;
	
	private QStat[] qstats;
	private int[] qwaitcnts;
	private int[] qrecvcnts;
	private QueryResults[][] qresults;
	private long time;
	
	private class QStat {
		int numNodes;
		int numTerms;
		long time;
		long procTime;
		long maxProcTime;
	}
	
	public DPQueryDispatcher(DPKernel kernel, int pre, int cnt, int maxconcurrent, boolean isand, String logsuffix){
		try {
			this.kernel = kernel;
			this.index = (MasterIndex)kernel.index;
			index.loadFastMaxScores(index.getStatistics().getNumberOfUniqueTerms());
			this.server = kernel.server;
			
			querylogReader = new BufferedReader(new FileReader(kernel.index.getPath()+"/querylog.test"+logsuffix));
			//querylogReader = new BufferedReader(new FileReader("/home/simonj/logs/querylog_cleaned_old"));
			preproc = new DPMasterQueryPreprocessing(DPKernel.testing ? index.getGlobalLexicon() : index.getInMemoryGlobalLexicon(), isand);
			outBuffer = new ArrayBlockingQueue<Pair<Integer, ChannelBuffer[]>>(maxconcurrent*4+50);
			inBuffer = new ArrayBlockingQueue<ChannelBuffer>(maxconcurrent);
			
			this.pre = pre;
			this.stopAfter = cnt+pre;
			
			remcnt = stopAfter;
			remslots = maxconcurrent;
			sent = 0;
		
			//System.out.println("pre " + pre + " stopAfter " + stopAfter + " remsnt " + remcnt);
			qstats = new QStat[cnt];
			qwaitcnts = new int[stopAfter];
			qrecvcnts = new int[stopAfter];
			qresults = new QueryResults[stopAfter][GlobalLexiconEntry.MAX_NODES_SUPPORTED];
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
		
	@Override
	public void run(){
		//start queryprereader
		//System.out.println("running dispatcher!");
		kernel.workpool.submit(new QueryPreReader());
		kernel.workpool.submit(new QueryResultsTasker());
		//kernel.workpool.submit(new QueryPreTest());
	}

	//private int queriesSinceLastFlush = 0;
	
	public MasterQuery nextQuery(){
		String querystr = null;
		try {
			querystr = querylogReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		if (querystr == null){
			System.out.println("EOF");
			return null;
		}
		/*if (++queriesSinceLastFlush > 1000){
			preproc.decrementLoads();
			queriesSinceLastFlush = 0;
		}*/
		return preproc.processQuery(querystr);
	}
	
	private long totalPreprocessingTime = 0l;
	
	private class QueryPreReader implements Runnable{
		@Override
		public void run() {
			int qid = 0;
			long start, end;
			while (!terminated && qid < stopAfter) {
				/*
				 * 1. convert to byte buffer.
				 * 2. find which node will receive
				 * 3. push into the queue
				 */
				MasterQuery mq;
				start = System.currentTimeMillis(); 
				do { mq = nextQuery(); } while (mq==null); //loop so you don't get any null-queries.
				end = System.currentTimeMillis();
				
				
				if (qid >= pre && qid < stopAfter){
					QStat qs = qstats[qid-pre] = new QStat();
					qs.numNodes = mq.numNodes;
					qs.numTerms = mq.numTerms;
					totalPreprocessingTime += end - start;
				}
				
				try {
					outBuffer.put(new Pair<Integer, ChannelBuffer[]>(qid, mq.toChannelBuffers(qid, 0)));	//merger set to 0
					//System.out.println(qid + ":" + sq.query);
					qid++;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}		
	}
	
	private AtomicLong totalPostprocessingTime = new AtomicLong();
	
	private class QueryResultsMergeTask implements Runnable {
		private int qid;
		
		public QueryResultsMergeTask(int qid){
			this.qid = qid;
		}
		
		@Override
		public void run() {
			long start = System.currentTimeMillis(); 
			QueryResultsIterator[] qres = new QueryResultsIterator[GlobalLexiconEntry.MAX_NODES_SUPPORTED];
			int numsubs = 0;
			for (QueryResults qr : qresults[qid]){
				if (qr!=null && qr.getNumberOfResults() > 0) {
					qres[numsubs] = new QueryResultsIterator(qr);
					qres[numsubs++].next();
				}
			}
			//System.out.println("merging " + numsubs);
			
			int docids[] = new int[Constants.MAX_NUMBER_OF_RESULTS];
			double scores[] = new double[Constants.MAX_NUMBER_OF_RESULTS];
			int numberOfResults = 0;
			
			int _best;
			while (numsubs > 0 && numberOfResults < Constants.MAX_NUMBER_OF_RESULTS){
				//find best candidate...
				_best = 0;
				for (int i=1; i<numsubs; i++)
					if (qres[i].getScore() > qres[_best].getScore())
						_best = i;
				
				//insert it into the final results...
				docids[numberOfResults] = qres[_best].getDocId();
				scores[numberOfResults] = qres[_best].getScore();
				numberOfResults++;
				
				//if iterator is finished, remove it...
				if (!qres[_best].next()){
					numsubs--;
					for (int j=_best; j<numsubs; j++)
						qres[j] = qres[j+1];
				}
			}
			long end = System.currentTimeMillis();
			
			/*
			StringBuilder sb = new StringBuilder();
			for (int i=0;i<numberOfResults;i++){
				sb.append("\n"+i+ " " + docids[i]+" "+scores[i]);
			}
			System.out.println(qid + sb.toString() + "\n" + (double)(end-start)/1000);
			*/
			
			totalPostprocessingTime.addAndGet(end-start);
			qresults[qid] = null;	//clean results...
			if (qid >= pre && qid < stopAfter){
				int relid = qid - pre;
				qstats[relid].time = end - qstats[relid].time; 
			}
			//System.out.println(qid + " finished!");
		}
	}
	
	private ExecutorService mergepool = Executors.newCachedThreadPool();
	
	private class QueryResultsTasker implements Runnable {
		@Override
		public void run() {			
			try {				
				ArrayList<ChannelBuffer> drain = new ArrayList<ChannelBuffer>();
				int qid;
				while (!terminated){
					drain.clear();
					inBuffer.drainTo(drain);					
					for (ChannelBuffer reply : drain){
						Triple<Integer, QueryResults, Long> p = RemoteQueryResults.fromChannelBuffer(reply);
						qid = p.getFirst(); 
						qresults[qid][qrecvcnts[qid]++] = p.getSecond();
						//System.out.println("recv " + qid + " " + qrecvcnts[qid]);

						//update qstats...
						if (qid >= pre && qid < stopAfter){
							QStat qstat = qstats[qid-pre];
							long procTime = p.getThird();
							qstat.procTime += procTime;
							if (qstat.maxProcTime < procTime) qstat.maxProcTime = procTime;
						}
						
						if (qrecvcnts[qid] == qwaitcnts[qid]) {
							remcnt--; remslots++;
							mergepool.submit(new QueryResultsMergeTask(qid));
							
							if (remcnt == 0) {
								//System.out.println("await termination");
								mergepool.shutdown();
								mergepool.awaitTermination(10, TimeUnit.MINUTES);
								//System.out.println("finished!");
								time = System.currentTimeMillis() - time;
								terminated = true;
								printStatsAndShutDown();
								return;
							}
						}
					}
					
					while (sent < stopAfter && remslots > 0){
						remslots--;
						sent++;
						
						Pair<Integer, ChannelBuffer[]> p = outBuffer.take();
						qid = p.getFirst();
						ChannelBuffer[] cb = p.getSecond();
						int numsubs = 0;
						for (int i = 0; i < cb.length; i++){
							if (cb[i] != null){
								server.write(i+1, cb[i]);
								numsubs++;
							}
							qwaitcnts[qid] = numsubs;
						}								
						
						if (qid==pre) time = System.currentTimeMillis();
						if (qid >= pre && qid < stopAfter) qstats[qid-pre].time = System.currentTimeMillis();
												
						//System.out.println("sending: " + qid + "( " + numsubs + ") remslots:" + remslots);					
						if (sent % 1000 == 0)
							System.out.println(sent);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
	}
	
	public void printStatsAndShutDown(){
		kernel.printInfo();
		int cnt = stopAfter-pre;
		System.out.println("res:QPS " + (double)(cnt*1000)/time);
		
		System.out.println("res:Avg.PreprocTime " + (double)totalPreprocessingTime/cnt);
		System.out.println("res:Avg.PostprocTime " + (double)totalPostprocessingTime.get()/cnt);
		
		long sumTimes = 0l;
		QStat qs;
		
		int[] qLenToNumNodes = new int[9];
		int[] qLenToNumQueries = new int[9];
		long[] qLenToTimes = new long[9];
		long[] qLenToProcTimes = new long[9];
		long[] qLenToMaxProcTimes = new long[9];
		int[] numNodesToNumQueries = new int[9];
		
		long sumProcTimes = 0l;
		long sumMaxProcTimes = 0l;
		
		int qlen, qsubs;
		for (int i=0; i<cnt; i++){
			qs = qstats[i];
			sumTimes+=qs.time;
			sumProcTimes += qs.procTime/qs.numNodes;
			sumMaxProcTimes += qs.maxProcTime;
			qlen = qs.numTerms;
			if (qlen > 8) qlen = 8;
			qsubs = qs.numNodes;
			
			qLenToNumQueries[qlen]++;
			qLenToNumNodes[qlen] += qsubs;
			qLenToTimes[qlen] += qs.time;
			qLenToProcTimes[qlen] += qs.procTime/qs.numNodes;
			qLenToMaxProcTimes[qlen] += qs.maxProcTime;
				
			numNodesToNumQueries[qsubs]++;
		}
		
		System.out.println("res:Lat " + (double)sumTimes/cnt);
		
		System.out.println("res:AvgProcTime " + (double)sumProcTimes/cnt);
		
		System.out.println("res:MaxProcTime " + (double)sumMaxProcTimes/cnt);
		
		System.out.println("res:qLenToFraction");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)qLenToNumQueries[i]/cnt);
		}
		System.out.println();
		
		System.out.println("res:qLenToNumNodes");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)qLenToNumNodes[i]/qLenToNumQueries[i]);
		}
		System.out.println();
		
		System.out.println("res:numNodesToFraction");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)numNodesToNumQueries[i]/cnt);
		}
		System.out.println();
		
		System.out.println("res:qLenToTime");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)qLenToTimes[i]/qLenToNumQueries[i]);
		}
		System.out.println();
		
		System.out.println("res:qLenToProcTime");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)qLenToProcTimes[i]/qLenToNumQueries[i]);
		}
		System.out.println();
		
		System.out.println("res:qLenToMaxProcTime");
		for (int i=0; i<=8; i++){
			System.out.println(i+"\t"+(double)qLenToMaxProcTimes[i]/qLenToNumQueries[i]);
		}
		System.out.println();
		server.shutdownAll();
		
		this.close();
	}
	
	@Override
	public void close(){
		try {
			querylogReader.close();
			preproc.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void messageReceived(Channel channel, ChannelBuffer buffer) {
		if (!terminated){
			try {
				inBuffer.put(buffer);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		}
	}
}