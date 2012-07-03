package com.ntnu.laika;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.preprocessing.SimpleQueryPreprocessing;
import com.ntnu.laika.query.processing.AND_DAAT_DOCIDS_ONLY_QueryProcessing;
import com.ntnu.laika.query.processing.QueryProcessing;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class IntersectionCountTest {
		
	public static void simpleTest(String path, int k) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = k;
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/workstuff/pair.train"));
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		SimpleQueryPreprocessing preproc = new SimpleQueryPreprocessing(index.getLexicon(stats.getNumberOfUniqueTerms()));
		QueryProcessing proc = new AND_DAAT_DOCIDS_ONLY_QueryProcessing(index.getInvertedIndex(), stats);
		System.out.println("..ok");
		String line; Query query;
		int i = 0;
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/workstuff/pair_out1.train"));
		while ((line = reader.readLine()) != null ) {
			query = preproc.processQuery(line);
			if (query == null || query.getEntries().length < 2){
				writer.write(line + "\t" + -1+"\n");
				continue;
			}
			QueryResults qr = proc.processQuery(query);
			int len = qr.getDocids().length;
			writer.write(line + "\t" + len+"\n");
			if (++i%1000 == 0) System.out.print(i+" ");
		}
		System.out.println("done!");
		writer.close();
		reader.close();
		index.close();
	}

	
	public static void main(String args[]) throws Exception{				
		String path = "/mnt/data/data/laika_v2/index/";
		int topK = 25000000;
		simpleTest(path, topK);
	}		
}
