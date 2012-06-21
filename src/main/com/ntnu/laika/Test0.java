package com.ntnu.laika;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.QueryResultsIterator;
import com.ntnu.laika.query.preprocessing.QueryPreprocessing;
import com.ntnu.laika.query.processing.*;
import com.ntnu.laika.runstats.SimpleStats;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.docdict.DocDict;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Test0 {
	
	public static void main(String[] args) throws IOException{
		SimpleStats.init("Candidates Inserted Into Heap",	//0
				 "Number Of Postings Scored",		//1
				 "Number Of Docids Evaluated",		//2
				 "Number Of Chunks Decoded",		//3
				 "Number Of Blocks Read",			//4
				 "Number Of Freqs Evaluated");		//5

		
		
		Index index = null;
		Statistics stats = null;
		QueryPreprocessing preproc = null;
		QueryProcessing proc = null;
		
		if (args.length == 0) {
			args = new String[5];
			args[0] = "0";
			args[1] = "0";
			args[2] = "1000";
			args[3] = "400000";
		//	args[4] = "16";
		}
		
		int idxcode = Integer.parseInt(args[0]);
		Constants.BUFFER_BLOCK_SIZE = 16*1024;
		//System.out.println("BufferSize:" + Constants.BUFFER_BLOCK_SIZE);
		Constants.MAX_NUMBER_OF_RESULTS = Integer.parseInt(args[2]);
		switch (idxcode) {
			case 0: 														//AND DAAT
				System.out.println("AND DAAT K=" + args[2]);
				Constants.USE_SKIPS = true;
				index = new Index("/home/simonj/ddata/laikatest");
				stats = index.getStatistics();
				index.loadFastDocLengths(stats.getNumberOfDocuments());		
				
				preproc = new QueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
				proc = new AND_DAAT_QueryProcessing(index.getInvertedIndex(), stats);
				break;
		}
		
		int testcode = Integer.parseInt(args[1]);
		switch (testcode) {
			case 0:
				runQualityTest(preproc, proc, index.getDocDict(stats.getNumberOfDocuments()));
				break;
			case 1:
				runPerformanceBase(preproc, proc, 10000);
				break;
			case 2: 
			default:
				runPerformanceTest(preproc, proc, 10000);
		}
		//index.getDocDict();
		index.close();
	}
	
	public static void runSingleTest(QueryPreprocessing preproc, QueryProcessing proc){		
		String querystr = "renting houses in new jersey";
		Query query;
		QueryResults results;
		long start, end;
		long totaltime = 0;
		
		System.out.println(querystr);
		query = preproc.processQuery(querystr);		

		start = System.currentTimeMillis();
		results = proc.processQuery(query);
		end = System.currentTimeMillis();
		totaltime += end - start;
			
		QueryResultsIterator resIter = new QueryResultsIterator(results);
		System.out.println(results.getNumberOfResults() +" out of " + results.getRealNumberOfResults());
		int j=0;
		while (resIter.next()){
			System.out.println(j++ + " " + resIter.getDocId() + " " + resIter.getScore());
		}

		System.out.println("time " + (double)totaltime / (1000));
	}
	
	public static void runPerformanceTest(QueryPreprocessing preproc, QueryProcessing proc, int count) throws IOException{
		BufferedReader querylogReader = new BufferedReader(new FileReader("/home/simonj/ddata/laikatest/querylog"));
		
		String querystr;
		Query query;
		long start, end;
		long totaltime = 0;
		
		for (int i=0; i<count; i++){
			querystr = querylogReader.readLine();
			if (i%1000 == 0) System.out.println(i);
		//	System.out.println(i+" "+querystr);
		//	System.out.println("preproc");
			query = preproc.processQuery(querystr);		
		//	System.out.println("proc");
			start = System.currentTimeMillis();
			proc.processQuery(query);
			end = System.currentTimeMillis();
			totaltime += end - start;
		//	System.out.println("done");
			/*
			QueryResultsIterator resIter = new QueryResultsIterator(results);
			System.out.println(results.getNumberOfResults() +" out of " + results.getRealNumberOfResults());
			int j=0;
			while (resIter.next()){
				System.out.println(j++ + " " + resIter.getDocId() + " " + resIter.getScore());
			}
			*/
		}
		System.out.println("average time " + (double)totaltime / (1000 * count));
		System.out.println(SimpleStats.getString());
		querylogReader.close();
	}
	public static void runPerformanceBase(QueryPreprocessing preproc, QueryProcessing proc, int count) throws IOException{
		BufferedReader querylogReader = new BufferedReader(new FileReader("/home/simonj/ddata/laikatest/querylog"));
		
		String querystr;
		for (int i=0; i<count; i++){
			querystr = querylogReader.readLine();
			//System.out.println(i+" "+querystr);
			if (i%1000 == 0) System.out.println(i);
			preproc.processQuery(querystr);		
		}
		System.out.println(SimpleStats.getString());
		querylogReader.close();
	}
	
	
	private static HashMap<String,HashSet<String>> loadAnswers(){
		HashMap<String,HashSet<String>> answers = new HashMap<String, HashSet<String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("/home/simonj/ddata/laikatest/qrels.tb06.top50"));
			String tmp, tmps[];
			while ( (tmp=br.readLine()) != null) {
				tmps = tmp.split(" ");
				if (!tmps[3].equals("0")){
					HashSet<String> set = answers.get(tmps[0]);
					if (set==null) {
						set = new HashSet<String>();
						answers.put(tmps[0], set);
					}
					set.add(tmps[2]);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return answers;
	}

	public static void runQualityTest(QueryPreprocessing preproc, QueryProcessing proc, DocDict docdict) throws IOException{
		int count = 0;
		double precsum = 0, recallsum = 0;
		
		HashMap<String,HashSet<String>> answers = loadAnswers();
		
		BufferedReader br = new BufferedReader(new FileReader("/home/simonj/ddata/laikatest/queries801-850"));
		String tmp, tmps[], querystr;
		Query query;
		QueryResults results;
		while ( (tmp=br.readLine()) != null) {
			tmps = tmp.split(":");
			if (answers.containsKey(tmps[0])){
				
				querystr = tmps[1];
				
				//System.out.println(querystr);
				query = preproc.processQuery(querystr);		
				results = proc.processQuery(query);
				
				QueryResultsIterator resIter = new QueryResultsIterator(results);
				
				
				int j=0;
				int corrno = 0;
				int numresults = results.getNumberOfResults();
				double accprec = 0.0;
				String docno;
				while (resIter.next()){
					docno = docdict.lookup(resIter.getDocId()).getDocno();
					if (answers.get(tmps[0]).contains(docno)){
						corrno++;
						accprec += ((double)corrno)/(j+1);
					}
					j++;
				}

				//precsum += (corrno == 0) ? 0 : accprec/corrno;
				precsum += (corrno == 0) ? 0 : accprec/answers.get(tmps[0]).size();
				recallsum += (numresults == 0) ? 0 :((double)corrno)/answers.get(tmps[0]).size();
				count++;
				System.out.println("|"+count);
			}
		}
		
		System.out.println("MAP:" + (precsum/count));
		System.out.println("Avg Recall:" + (recallsum/count));
	}
	
}
