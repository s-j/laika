package com.ntnu.laika.distributed.tp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import com.ntnu.laika.distributed.tp.TPMasterQueryPreprocessingHPQP.MasterQuery;
import com.ntnu.laika.distributed.util.RemoteQueryResults;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.laika.utils.Triple;
import com.ntnu.network.MessageHandler;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPQueryDispatcherHPQP implements Runnable, Closeable, MessageHandler{
	
	private BufferedReader querylogReader;
	private TPMasterQueryPreprocessingHPQP preproc;
	
	private TPKernelHPQP kernel;
	private MasterIndex index;
	private NetworkServer server;
	
	private ArrayBlockingQueue<Triple<Integer, Integer, ChannelBuffer>> outBuffer;
	private ArrayBlockingQueue<ChannelBuffer> inBuffer;
	private int remcnt, remslots, sent;
	
	private int pre, stopAfter;
	private volatile boolean terminated = false;
	
	private QStat[] qstats;
	private long time;
	
	private class QStat {
		int numNodes;
		int numTerms;
		long time;
		long procTime;
	}
	
	public TPQueryDispatcherHPQP(TPKernelHPQP kernel, int pre, int cnt, int maxconcurrent, String logsuffix){
		try {
			this.kernel = kernel;
			this.index = (MasterIndex)kernel.index;
			index.loadFastMaxScores(index.getStatistics().getNumberOfUniqueTerms());
			this.server = kernel.server;
			
			querylogReader = new BufferedReader(new FileReader(kernel.index.getPath()+"/querylog.test"+logsuffix));
			//querylogReader = new BufferedReader(new FileReader("/home/simonj/logs/querylog_cleaned_old"));
			preproc = new TPMasterQueryPreprocessingHPQP(TPKernelHPQP.testing ? index.getGlobalLexicon() : index.getInMemoryGlobalLexicon());
			outBuffer = new ArrayBlockingQueue<Triple<Integer,Integer,ChannelBuffer>>(maxconcurrent*4+50);
			inBuffer = new ArrayBlockingQueue<ChannelBuffer>(maxconcurrent);
			
			this.pre = pre;
			this.stopAfter = cnt+pre;
			
			remcnt = stopAfter;
			remslots = maxconcurrent;
			sent = 0;
		
			//System.out.println("pre " + pre + " stopAfter " + stopAfter + " remsnt " + remcnt);
			qstats = new QStat[cnt];
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
		if (querystr == null) return null;
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
				MasterQuery sq;
				start = System.currentTimeMillis(); 
				do { sq = nextQuery(); } while (sq==null); //loop so you don't get any null-queries.
				end = System.currentTimeMillis();
				
				
				if (qid >= pre && qid < stopAfter){
					QStat qs = qstats[qid-pre] = new QStat();
					qs.numNodes = sq.numNodes;
					qs.numTerms = sq.numTerms;
					totalPreprocessingTime += end - start;
				}
				
				try {
					outBuffer.put(new Triple<Integer, Integer, ChannelBuffer>(sq.getFirstNode(), qid, sq.toChannelBuffer(qid)));
					//System.out.println(qid + ":" + sq.query);
					qid++;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}		
	}
	
	private class QueryResultsTasker implements Runnable {
		//private ChannelBuffer reply;
		
		@Override
		public void run() {
			try {				
				ArrayList<ChannelBuffer> drain = new ArrayList<ChannelBuffer>();
				int qid;
				while (!terminated){
					drain.clear();
					inBuffer.drainTo(drain);					
					for (ChannelBuffer reply : drain){
						remcnt--; remslots++;
						Triple<Integer, QueryResults, Long> p = RemoteQueryResults.fromChannelBuffer(reply);
						qid = p.getFirst();
						/*
						QueryResults results = p.getSecond();
						StringBuffer sb = new StringBuffer(qid+" "+results.getNumberOfResults()+ " "+results.getRealNumberOfResults());
						int docids[] = results.getDocids();
						double scores[] = results.getScores();
						int nump = docids.length < 100 ? docids.length : 100; 
						for (int i=nump-5;i<nump;i++){
							sb.append("\n"+i+ " " + docids[i]+" "+scores[i]);
						}
						System.out.println(sb.toString());
						*/
						if (qid >= pre && qid < stopAfter){
								int relid = qid - pre;
								qstats[relid].time = System.currentTimeMillis() - qstats[relid].time;
								qstats[relid].procTime = p.getThird(); 
						}
						
						//if (qid > 14000) 
						//System.out.println("got " + qid + " rem " + remcnt + " slots " + remslots);
						
						if (remcnt == 0) {
							time = System.currentTimeMillis() - time;
							terminated = true;
							printStatsAndShutDown();
							return;
						}
					}
					
					while (sent < stopAfter && remslots > 0){
						remslots--;
						sent++;
						
						Triple<Integer, Integer, ChannelBuffer> p = outBuffer.take();
						server.write(p.getFirst(), p.getThird());
																
						qid = p.getSecond();
						if (qid==pre) time = System.currentTimeMillis();
						if (qid >= pre && qid < stopAfter) qstats[qid-pre].time = System.currentTimeMillis();
												
						//System.out.println("sending " + qid + " remslots " + remslots);					
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
		
		long sumTimes = 0l;
		QStat qs;
		
		int[] qLenToNumNodes = new int[9];
		int[] qLenToNumQueries = new int[9];
		long[] qLenToTimes = new long[9];
		long[] qLenToProcTimes = new long[9];
		int[] numNodesToNumQueries = new int[9];
		
		long sumProcTimes = 0l;
		
		int qlen, qsubs;
		for (int i=0; i<cnt; i++){
			qs = qstats[i];
			sumTimes+=qs.time;
			sumProcTimes += qs.procTime;
			qlen = qs.numTerms;
			if (qlen > 8) qlen = 8;
			qsubs = qs.numNodes;
			
			qLenToNumQueries[qlen]++;
			qLenToNumNodes[qlen] += qsubs;
			qLenToTimes[qlen] += qs.time;
			qLenToProcTimes[qlen] += qs.procTime;
				
			numNodesToNumQueries[qsubs]++;
		}
		
		System.out.println("res:Lat " + (double)sumTimes/cnt);
		
		System.out.println("res:ProcTime " + (double)sumProcTimes/cnt);
		
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