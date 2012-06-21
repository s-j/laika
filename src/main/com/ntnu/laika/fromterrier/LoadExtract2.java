package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LoadExtract2 {
	private static class Counter{
		int id;
		int c = 0;
		public Counter(int id){
			this.id = id;
		}
	}
	
	public static void main(String args[]) throws Exception{				
		String path = "/mnt/data/data/laika_v2/index/";
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/2Enver/querylog_cleaned_learn"));
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		Lexicon lexicon = index.getLexicon(stats.getNumberOfUniqueTerms());
		HashMap<String, Counter> counts = new HashMap<String, Counter>();
		
		String line; int i=0;
		long ftacc = 0; int uterms = 0;
		while ((line = reader.readLine()) != null ) {
			boolean trip = false;
			for (String token : line.split(" ")){
				LexiconEntry _lEntry = lexicon.lookup(token);	
				if (_lEntry != null){
					if (FastMaxScore.getMaxScore(_lEntry.getTermId())>0.0d) {
						Counter c = counts.get(token);
						if (c==null) {
							c = new Counter(_lEntry.getTermId());
							counts.put(token, c);
							ftacc += _lEntry.getN_t();
							uterms++;
						}
						c.c++;
						trip = true;
					}
				}
			}
			if (trip){
				if (++i % 1000 == 0) System.out.println(i);
			}
		}
		reader.close();
		System.out.println("x");
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/2Enver/counts"));
		int max = 0; int sum = 0;
		writer.write(counts.size()+"\n");
		for (Entry<String, Counter> e : counts.entrySet()){
			int val = e.getValue().c;
			writer.write(e.getValue().id + "\t" + val + "\n");
			if (val > max) max = val;
			sum+=val;
		}
		writer.close();
		System.out.println("#max: " + max + " avg: " + (double)sum/counts.size() + " f_tavg: " + (double)ftacc/uterms);
		index.close();
	}		
}
