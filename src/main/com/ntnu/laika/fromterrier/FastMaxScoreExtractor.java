package com.ntnu.laika.fromterrier;

import java.io.IOException;
import com.ntnu.laika.Constants;
import com.ntnu.laika.query.processing.scoremodels.BM25;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.docdict.FastDocLength;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScoreOutputStream;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FastMaxScoreExtractor {
	
	public FastMaxScoreExtractor(String path) throws IOException{
		Index index = new Index(path);
		Statistics stats = index.getStatistics();
		//Constants.USE_SKIPS = true;
		BM25 wmodel = new BM25();
		wmodel.setAverageDocumentLength(stats.getAverageDocumentLength());
		wmodel.setNumberOfDocuments(stats.getNumberOfDocuments());
		
		index.loadFastDocLengths(stats.getNumberOfDocuments());
		
		PostingListInputStream plis = index.getPostingListInputStream(stats.getNumberOfUniqueTerms());
		FastMaxScoreOutputStream fmos = index.getFastMaxScoreOutputStream();

		int numTerms = index.getStatistics().getNumberOfUniqueTerms();
		LexiconEntry lEntry;
		System.out.println(Constants.USE_SKIPS + " " + Constants.STRING_BYTE_LENGTH);
		double maxscore, _score, precomputed;
		//BufferedWriter bw = new BufferedWriter(new FileWriter("/home/simonj/distr"));
		for (int i=0; i<numTerms; i++){
			lEntry = plis.nextEntry();
			wmodel.setDocumentFrequency((double)lEntry.getN_t());
			wmodel.setTermFrequency((double)lEntry.getTF());
			wmodel.setKeyFrequency(1.0d);
			precomputed = wmodel.precompute();
			maxscore = 0.0d;
			do {
				_score = wmodel.score(plis.getFrequency(), FastDocLength.getNumberOfTokens(plis.getDocId()), precomputed);
				if (maxscore < _score) maxscore = _score;
			} while (plis.next());
			fmos.nextEntry(lEntry.getTermId(), maxscore);
			//bw.write(lEntry.getN_t() + "\t" + maxscore+ "\n");
			if (i % 1000000 == 0) System.out.println(i);
		}
		
		fmos.close();
		index.close();
		//bw.close();
	}
	
	public static void main(String args[]) throws IOException{
		Constants.USE_SKIPS = true;
		System.out.println("---");
		String inpath = "/mnt/data/data/laika_v2/index";
		if (args.length > 0) inpath = args[0];
		new FastMaxScoreExtractor(inpath);
	}
}
