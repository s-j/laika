package com.ntnu.laika;

import java.io.BufferedReader;
import java.io.FileReader;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.preprocessing.SimpleQueryPreprocessing;
import com.ntnu.laika.query.processing.AND_DAAT_QueryProcessing;
import com.ntnu.laika.query.processing.QueryProcessing;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Test2 {
		
	public static void simpleTest(String path, int k) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = k;
		BufferedReader reader = new BufferedReader(new FileReader("/mnt/data/data/ENVERIDX/querylog.test"));
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		QueryProcessing proc = new AND_DAAT_QueryProcessing(index.getInvertedIndex(), stats);
		System.out.println("..ok");
		String line; Query query;
		int i = 0;
		while ((line = reader.readLine()) != null ) {
			query = preproc.processQuery(line);
			if (query == null) continue;
			System.out.println(i+" "+line);
			QueryResults qr = proc.processQuery(query);
			/*int[] d = qr.getDocids();
			double[] s = qr.getScores();
			for (int j=0; j< d.length; j++){
				System.out.println(j +  "\t" + d[j] + "\t" + s[j]);
			}
			System.out.println();
			if (++i%1000 == 0) System.out.print(i+" ");*/
			if (++i==30000) break;
		}
		System.out.println("done!");
		index.close();
	}

	
	public static void main(String args[]) throws Exception{				
		String path = "/mnt/data/data/laika_v2/index/";
		int topK = 100;
		simpleTest(path, topK);
	}		
}
