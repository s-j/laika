package com.ntnu.laika;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.preprocessing.SimpleQueryPreprocessing;
import com.ntnu.laika.query.processing.AND_DAAT_LP_MaxScore2_QueryProcessing;
import com.ntnu.laika.query.processing.AND_DAAT_LP_MaxScore_QueryProcessing;
import com.ntnu.laika.query.processing.AND_DAAT_MaxScore_QueryProcessing;
import com.ntnu.laika.query.processing.OR_DAAT_LP_MaxScore_QueryProcessing;
import com.ntnu.laika.query.processing.QueryProcessing;
import com.ntnu.laika.runstats.SimpleMultiStats;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.lpsolver.AccScorePredictor;
import com.ntnu.lpsolver.LPSolver;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LPTest {
	
	private static class Counter {
		int c;
		Counter (int _c) {c=_c;}
		final int get(){return c;}
		final void increment(){c++;}
	}
	
	public static void removeDuplicates() throws Exception{
		HashSet<String> cache = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/work/LP_data/querylog_cleaned"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/work/LP_data/querylog_cleaned_nodupes"));
		String line;
		while((line = reader.readLine())!=null){
			if (cache.add(line)) writer.write(line+"\n");
		}
		reader.close();
		writer.close();
		System.out.println("done");
	}
	
	public static void split() throws Exception{
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/work/LP_data/querylog_cleaned_nodupes"));
		int i=0;
		String line;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/work/LP_data/querylog_cleaned_learn"));
		while(i<80000 && (line = reader.readLine())!=null){
			writer.write(line+"\n");
			i++;
		}
		writer.close();
		
		writer = new BufferedWriter(new FileWriter("/home/simonj/work/LP_data/querylog_cleaned_test"));
		while((line = reader.readLine())!=null){
			writer.write(line+"\n");
		}
		writer.close();
		
		reader.close();
		System.out.println("done");
	}
	
	public static void allPairs() throws Exception{
		HashMap<String, Counter> pairs = new HashMap<String,Counter>(200000);
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/work/LP_data/querylog_cleaned_learn"));
		String line, pair;
		ArrayList<String> uniqueTerms = new ArrayList<String>();
		while((line = reader.readLine())!=null){
			//remove dupes
			for (String s : line.split(" ")){
				if (uniqueTerms.size() == 0 || !uniqueTerms.get(uniqueTerms.size()-1).equals(s)){
					uniqueTerms.add(s);
				}
			}
			
			int numTerms = uniqueTerms.size();
			
			Counter c;
			for (int i=0; i < numTerms - 1; i++){
				for (int j = i + 1; j < numTerms; j++){
					pair = uniqueTerms.get(i) + " " + uniqueTerms.get(j);
					c = pairs.get(pair);
					if (c!=null) c.increment();
					else pairs.put(pair, new Counter(1));
				}
			}
			uniqueTerms.clear();
		}
		
		
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/work/LP_data/allpairs"));
		
		for (Entry<String, Counter> e : pairs.entrySet()){
			writer.write(e.getKey() + "\t"+e.getValue().get()+"\n");
		}
		writer.close();
		
		System.out.println("done - " + pairs.size());
		
	}
	
	public static void calculatePairScores() throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = 1;
		Index index = new Index("/home/simonj/laikatest");
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		//QueryProcessing proc = new AND_DAAT_QueryProcessing(index.getInvertedIndex(), stats);
		QueryProcessing proc = new AND_DAAT_LP_MaxScore_QueryProcessing(index.getInvertedIndex(), stats);
		
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/work/LP_data/allpairs_sorted"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/work/LP_data/allpairs_scored"));
		
		String line, querystr; Query query; QueryResults results; QueryEntry []qentries;
		LexiconEntry l1, l2;
		double maxscore, scores[]; 
		
		int i = 0;
		while ( (line = reader.readLine()) != null ) {
			querystr = line.split("\t")[0];
			query = preproc.processQuery(querystr);
			if (query==null || query.getNumberOfTerms() < 2) continue; 
			//start = System.currentTimeMillis();
			results = proc.processQuery(query);
			//end = System.currentTimeMillis();
			scores = results.getScores();
			qentries = query.getEntries();
			l1 = qentries[0].getLexiconEntry();
			l2 = qentries[1].getLexiconEntry();
			maxscore = scores.length > 0 ? scores[0] : 0.0d;
			//System.out.println(i++ + " : " + querystr + " : " + l1.getTermId() + " " + l2.getTermId() + " --- " + maxscore);
			writer.write(querystr + "\t" + l1.getTermId() + " " + l2.getTermId() + "\t" + maxscore + "\n");
			if (++i % 1000 == 0) System.out.println(i);
		}
		System.out.println("done!");
		reader.close();
		writer.close();
		index.close();
	}
	
	public static void calculatePairScores(String path, String input, String output) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = 1;
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		QueryProcessing proc = new AND_DAAT_MaxScore_QueryProcessing(index.getInvertedIndex(), stats);
		
		BufferedReader reader = new BufferedReader(new FileReader(input));
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		
		String line; Query query; QueryResults results; QueryEntry []qentries;
		LexiconEntry l1, l2;
		double maxscore, scores[]; 
		
		int i = 0;
		while ( (line = reader.readLine()) != null ) {
			query = preproc.processQuery(line);
			if (query==null || query.getNumberOfTerms() < 2) continue; 
			//start = System.currentTimeMillis();
			results = proc.processQuery(query);
			//end = System.currentTimeMillis();
			scores = results.getScores();
			qentries = query.getEntries();
			l1 = qentries[0].getLexiconEntry();
			l2 = qentries[1].getLexiconEntry();
			maxscore = scores.length > 0 ? scores[0] : 0.0d;
			writer.write(l1.getTerm() + " " + l2.getTerm() + "\t" + l1.getTermId() + " " + l2.getTermId() + "\t" + maxscore + "\n");
			if (++i % 1000 == 0) System.out.println(i);
		}
		System.out.println("done!");
		reader.close();
		writer.close();
		index.close();
	}
	
	public static int longestLine(String path, int cnt) throws Exception{
		cnt *= 1.1;
		BufferedReader reader = new BufferedReader(new FileReader(path + "/test.queries"));
		String line;
		int max = 0, len;
		int i=0;
		while ( (line = reader.readLine()) != null ) {
			len = line.split(" ").length;
			if (max < len) max = len;
			if (i++ > cnt) break;
		}
		reader.close();
		return max;
	}
	
	public static void simpleTest(String path, int k, int stopafter, int qptype, boolean doLP) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = k;
		AND_DAAT_LP_MaxScore2_QueryProcessing.doLP =
				AND_DAAT_LP_MaxScore_QueryProcessing.doLP =
					OR_DAAT_LP_MaxScore_QueryProcessing.doLP = doLP;
		
		AccScorePredictor.loadData(path + "/allpairs_scored");
		BufferedReader reader = new BufferedReader(new FileReader(path+"/test.queries"));
		
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		System.out.println("..ok");
		QueryProcessing proc = null;
		if (qptype==0) proc = new AND_DAAT_LP_MaxScore_QueryProcessing(index.getInvertedIndex(), stats);
		else if (qptype==1) proc = new AND_DAAT_LP_MaxScore2_QueryProcessing(index.getInvertedIndex(), stats);
		else proc = new OR_DAAT_LP_MaxScore_QueryProcessing(index.getInvertedIndex(), stats);
		
		LPSolver.MAX_QUERY_TERM_COUNT = 7;
		int numTermsToTrack =  LPSolver.MAX_QUERY_TERM_COUNT + 1;
		String[] rowtitles = new String[numTermsToTrack];
		for (int i=0; i<numTermsToTrack; i++) rowtitles[i] = "" + (i+1);
		SimpleMultiStats.init(rowtitles,"blocks","chunks","scores","hpins");
		int querycounts[] = new int[numTermsToTrack];
		double[][] querytimes = new double[numTermsToTrack][3]; 
		
		String line; Query query;
		
		int i = 0;
		long time, elapsed = 0;
		int numterms;
		
		int kresults = 0;
		while ((line = reader.readLine()) != null ) {
			SimpleMultiStats.disable();
			query = preproc.processQuery(line);
			SimpleMultiStats.enable();
			if (query == null) continue;
			
			numterms = query.getNumberOfTerms();
			if (numterms > numTermsToTrack){
				numterms = numTermsToTrack;
			}
			SimpleMultiStats.setContext(numterms-1);
			
			time = System.nanoTime();
			QueryResults res = proc.processQuery(query);
			time = System.nanoTime() - time;
			
			if (res.getNumberOfResults() >= k) kresults++;
			
			elapsed += time;
			querycounts[numterms-1]++;
			querytimes[numterms-1][0] += time;
			querytimes[numterms-1][1] += proc.accScoreTime;
			querytimes[numterms-1][2] += proc.procLoopTime;
			proc.accScoreTime = 0l;
			proc.procLoopTime = 0l;
			
			/*
			StringBuffer sb = new StringBuffer(i+" "+results.getNumberOfResults()+ " "+results.getRealNumberOfResults());
			int docids[] = results.getDocids();
			scores = results.getScores();
			int nump = docids.length < k ? docids.length : k; 
			for (int j=0;j<nump;j++){
				sb.append("\n"+j+ " " + docids[j]+" "+scores[j]);
			}
			System.out.println(sb.toString());
			*/
			if (++i%1000 == 0) System.out.print((i*100)/stopafter+" ");
			if (i==stopafter) break;
		}
		System.out.println();
		DecimalFormat df = new DecimalFormat("#.###");
		System.out.println("done " + i + " queries, lat: " + df.format((double)elapsed/i/1000000) + " K-number: " + (double)kresults/i);
		reader.close();
		index.close();
		
		int cnt = i;
		System.out.println("#NumTerms to freq:");
		for (i=0; i<numTermsToTrack; i++){
			System.out.println(rowtitles[i]+"\t"+(double)querycounts[i]/cnt);
		}
		
		
		System.out.println("#Times (ms):");
		for (i=0; i<numTermsToTrack; i++){
			System.out.println(rowtitles[i]+"\t"+
				df.format((double)querytimes[i][0]/querycounts[i]/1000000) + "\t"+
				df.format((double)querytimes[i][1]/querycounts[i]/1000000) + "\t"+
				df.format((double)querytimes[i][2]/querycounts[i]/1000000) 
			);
		}
		
		System.out.println("#Stats:");
		System.out.println(SimpleMultiStats.getString(querycounts));
	}
	
	public static void main0(String args[]) throws Exception{
		calculatePairScores(args[0],args[1],args[2]);
	}
	
	public static void main(String args[]) throws Exception{				
		String path = args[0];
		int topK = Integer.parseInt(args[1]);
		int numQ = Integer.parseInt(args[2]);
		int qptype = Integer.parseInt(args[3]);
		boolean doLP = Boolean.parseBoolean(args[4]);
		Constants.MAX_QUERY_LENGTH = longestLine(path, numQ);
		//"/home/simonj/laikatest/"
		System.out.println("Running: top-" + topK +" #" + numQ + (qptype==0?" AND":(qptype==1?" AND2":" OR")) + " LP:" + doLP);
		simpleTest(path, topK, numQ, qptype, doLP);
	}		
}
