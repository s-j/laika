package com.ntnu.laika.fromterrier;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.docdict.DocDictEntry;
import com.ntnu.laika.structures.docdict.DocDictInputStream;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TestTest {
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/mnt/data/data/laika_v2/index/";

		Index input_index = new Index(inpath);
		Statistics input_stats = input_index.getStatistics();
		int numterms = input_stats.getNumberOfUniqueTerms();
		int numdocs = input_stats.getNumberOfDocuments();
		PostingListInputStream plis = input_index.getPostingListInputStream(numterms); 
		
		for (int i=0; i<2; i++){
			LexiconEntry _lE = plis.nextEntry();			
			int nt = _lE.getN_t();
			System.out.println(_lE.toString());
		}
		System.out.println("===");
		plis.close();

		
		System.out.println("done converting index!");		
		DocDictInputStream dis = input_index.getDocDictInputStream();
		for (int i=0; i<2; i++){
			DocDictEntry dde = dis.nextEntry();
			System.out.println(dde.getDocid() + " " + dde.getDocno() + " " + dde.getNumberOfTokens());
		}
		input_index.close();
		System.out.println("done!");
	}
}
