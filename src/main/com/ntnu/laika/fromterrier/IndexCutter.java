package com.ntnu.laika.fromterrier;

import java.io.IOException;
import java.util.Arrays;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.docdict.DocDictEntry;
import com.ntnu.laika.structures.docdict.DocDictInputStream;
import com.ntnu.laika.structures.docdict.DocDictOutputStream;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.LexiconOutputStream;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;
import com.ntnu.laika.structures.postinglist.PostingListOutputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class IndexCutter {
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/mnt/data/data/laika_v2/index/";
		String outpath ="/home/simonj/index4/";
		int stopafter = 25205179/4;
		Index input_index = new Index(inpath);
		Statistics input_stats = input_index.getStatistics();
		int numterms = input_stats.getNumberOfUniqueTerms();
		PostingListInputStream plis = input_index.getPostingListInputStream(numterms); 
		
		Index output_index = new Index(outpath);
		PostingListOutputStream plos = output_index.getPostingListOutputStream();
		LexiconOutputStream los = output_index.getLexiconOutputStream();
		Statistics output_stats = output_index.getStatistics();
		
		for (int i=0; i<numterms; i++){
			LexiconEntry _lE = plis.nextEntry();
			String term = _lE.getTerm();
			int nt = _lE.getN_t();
			int tf = 0;
						
			int[][] scores = new int[2][nt];
			for (int k=0; k<nt; k++){
				if (plis.getDocId() >= stopafter){
					nt=k;
					break;
				}
				scores[0][k] = plis.getDocId();
				scores[1][k] = plis.getFrequency();
				tf += plis.getFrequency();
				plis.next();
			}
			
			if (nt > 0) {
				int[][] _scores = new int[2][nt];
				_scores[0] = Arrays.copyOf(scores[0],nt);
				_scores[1] = Arrays.copyOf(scores[1],nt);
				
				LexiconEntry lEntry = plos.nextEntry(term, nt, tf, _scores);
				los.nextEntry(lEntry);
			}
			
			if (i % 1000000 == 0) System.out.println(i);
		}
		
		output_stats.setNumberOfDocuments(stopafter);
		output_stats.setNumberOfTokens(los.getNumberOfTokens());
		output_stats.setNumberOfPointers(los.getNumberOfPointers());
		output_stats.setNumberOfUniqueTerms(los.getNumberOfTerms());
		plis.close();
		plos.close();
		los.close();
		System.out.println("done converting index!");		
		DocDictInputStream ddis = input_index.getDocDictInputStream(); 
		DocDictOutputStream ddos = output_index.getDocDictOutputStream();
		for (int i=0; i<stopafter; i++){
			DocDictEntry dde = ddis.nextEntry();
			ddos.nextEntry(dde.getDocid(), dde.getDocno(), dde.getNumberOfTokens());
		}
		ddis.close();
		ddos.close();
		input_index.close();
		output_index.close();
		System.out.println("done converting docdict!");
		new FastMaxScoreExtractor(outpath);
		System.out.println("done converting maxscores!");
	}
}
