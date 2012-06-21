package com.ntnu.laika.distributed.tp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import com.ntnu.laika.distributed.tp.TPMasterQueryPreprocessing.MasterQuery;
import com.ntnu.laika.distributed.util.QualityChecker;
import com.ntnu.laika.distributed.util.RemoteQueryResults;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.laika.utils.Triple;
import com.ntnu.network.MessageHandler;
import com.ntnu.network.NetworkServer;

public class TPQueryDispatcher2 implements Runnable, Closeable, MessageHandler{
	
	private BufferedReader querylogReader;
	private TPMasterQueryPreprocessing preproc;
	
	private TPKernel kernel;
	private MasterIndex index;
	private NetworkServer server;
	
	private ArrayBlockingQueue<Triple<Integer, Integer, ChannelBuffer>> outBuffer;
	private ArrayBlockingQueue<ChannelBuffer> inBuffer;
	private int remcnt, remslots, sent;
	
	private int pre, stopAfter;
	private volatile boolean terminated = false;
	
	private long[] startTimes;
	private long startTime;
	
	private QualityChecker checker;
	
	public TPQueryDispatcher2(TPKernel kernel, int pre, int cnt, int maxconcurrent, boolean routeByMaxScore){
		try {
			this.kernel = kernel;
			this.index = (MasterIndex)kernel.index;
			index.loadFastMaxScores(index.getStatistics().getNumberOfUniqueTerms());
			this.server = kernel.server;
			
			querylogReader = new BufferedReader(new FileReader("/mnt/data/data/laika_v2/index/quality.log"));
			preproc = new TPMasterQueryPreprocessing(index.getGlobalLexicon(), routeByMaxScore);
			outBuffer = new ArrayBlockingQueue<Triple<Integer,Integer,ChannelBuffer>>(maxconcurrent*2);
			inBuffer = new ArrayBlockingQueue<ChannelBuffer>(maxconcurrent);
			
			this.pre = pre;
			this.stopAfter = cnt+pre;
			
			remcnt = stopAfter;
			remslots = maxconcurrent;
			sent = 0;
			
			checker = new QualityChecker(index);
			//System.out.println("pre " + pre + " stopAfter " + stopAfter + " remsnt " + remcnt);
			this.startTimes = new long[cnt];
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
	}

	public MasterQuery nextQuery(){	
		String querystr = null;
		
		try {
			querystr = querylogReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (querystr == null) return null;
		
		return preproc.processQuery(querystr);
	}
	
	private class QueryPreReader implements Runnable{
		@Override
		public void run() {
			int qid = 0;
			while (!terminated && qid < stopAfter) {
				/*
				 * 1. convert to byte buffer.
				 * 2. find which node will receive
				 * 3. push into the queue
				 */
				MasterQuery sq;
				
				do { sq = nextQuery(); } while (sq==null); //loop so you don't get any null-queries.
				
				//System.out.println("buffering a query!");
				
				try {
					outBuffer.put(new Triple<Integer, Integer, ChannelBuffer>(sq.getFirstNode(), qid, sq.toChannelBuffer(qid)));
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
						QueryResults results = p.getSecond();
						//long processingTime = p.getThird();
						checker.checkResults(qid+"", results);
						
						/* DO anything you want with results :)
						System.out.println(qid+" "+results.getNumberOfResults()+ " "+results.getRealNumberOfResults());						
						int docids[] = results.getDocids();
						double scores[] = results.getScores();
						for (int i=0;i<docids.length;i++){
							System.out.println(i+ " " + docids[i]+" "+scores[i]);
						}*/
						
						if (qid >= pre && qid < stopAfter)
								startTimes[qid - pre] = System.currentTimeMillis() - startTimes[qid - pre];
						
						//if (qid > 14000) 
						//	System.out.println("got " + qid + " rem " + remcnt + " slots " + remslots);
						
						if (remcnt == 0) {
							startTime = System.currentTimeMillis() - startTime;
							terminated = true;
							checker.printResults();
							printStatsAndShutDown();
							return;
						}
					}
					
					while (sent < stopAfter && remslots > 0){
						remslots--;
						sent++;
						
						Triple<Integer, Integer, ChannelBuffer> p = outBuffer.take();
						//System.out.println("readysteadygo");
						server.write(p.getFirst(), p.getThird());
																
						qid = p.getSecond();
						if (qid==pre) startTime = System.currentTimeMillis(); 
						if (qid >= pre && qid < stopAfter) startTimes[qid-pre] = System.currentTimeMillis();
						
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
		System.out.println("Avg.QPS " + (double)(cnt*1000)/startTime);
		long sumTimes = 0l;
		for (int i=0; i<startTimes.length; i++){
			sumTimes+=startTimes[i];
		}
		System.out.println("Avg.Lat " + (double)sumTimes/cnt);
		
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
