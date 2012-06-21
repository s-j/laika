package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.FileReader;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LoadCompute {
	public static void main(String args[]) throws Exception{				
		String path = "/mnt/data/data/laika_v2/index/";
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/countsfullq"));
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		index.loadFastMaxScores(stats.getNumberOfUniqueTerms());
		Lexicon lexicon = index.getLexicon(stats.getNumberOfUniqueTerms());
		
		String line; int i=0;
		reader.readLine();
		double fqavg = 1.137;
		double ftavg = 997967.6;		
		double covsum=0;
		double loadsum=0;
		int maxft = 0;
		int cnt = 0;
		while ((line = reader.readLine()) != null ) {
			String tmp[] = line.split("\t");
			LexiconEntry _lEntry = lexicon.lookup(tmp[0]);
			int ft = _lEntry.getN_t();
			covsum += (ft - ftavg) * (Integer.parseInt(tmp[1])-fqavg);
			loadsum += ft;
			if (maxft < ft){
				maxft = ft;
				System.out.println(tmp[0]);
			}
			cnt++;
		}
		reader.close();
		System.out.println("max: " + maxft + " avg: " + loadsum/cnt + " covariance: " + covsum/(cnt-1));
		index.close();
	}		
}
