package com.ntnu.laika;

import java.io.BufferedReader;
import java.io.FileReader;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.preprocessing.SimpleQueryPreprocessing;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Test1 {
		
	public static void simpleTest(String path, int k) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = k;
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/logs/querylog_cleaned_old1"));
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		System.out.println("..ok");
		String line; Query query;
		int i = 0;
		while ((line = reader.readLine()) != null ) {
			query = preproc.processQuery(line);
			if (query == null) continue;
			i++;
		}
		System.out.println(i);

	}

	
	public static void main(String args[]) throws Exception{				
		String path = "/mnt/data/data/laika_v2/index/";
		int topK = 100;
		simpleTest(path, topK);
	}		
}
