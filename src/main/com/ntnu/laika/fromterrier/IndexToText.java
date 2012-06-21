package com.ntnu.laika.fromterrier;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class IndexToText {
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/mnt/data/data/laika_v2/index/";

		Index input_index = new Index(inpath);
		Statistics input_stats = input_index.getStatistics();
		int numterms = input_stats.getNumberOfUniqueTerms();
		int numdocs = input_stats.getNumberOfDocuments();
		PostingListInputStream plis = input_index.getPostingListInputStream(numterms); 
		
		BufferedWriter buf = new BufferedWriter(new FileWriter("/mnt/data/data/2Enver/term2docmatrix"));
		buf.write(numterms + "\n");
		int[] doc2numPointers = new int[numdocs];
		for (int i=0; i<numterms; i++){
			LexiconEntry _lE = plis.nextEntry();			
			int nt = _lE.getN_t();
			buf.write(_lE.getTermId() + "\t" + _lE.getN_t() + "\t");
			for (int k=0; k<nt; k++){
				buf.write(plis.getDocId()+"");
				doc2numPointers[plis.getDocId()]++;
				buf.write(k<nt-1?' ':'\n');
				plis.next();
			}
			if (i%1000000 == 0) System.out.println(i);
		}
		
		plis.close();
		buf.close();

		
		System.out.println("done converting index!");		
		
		buf = new BufferedWriter(new FileWriter("/mnt/data/data/2Enver/docinfo2"));
		buf.write(numdocs + "\n");
		for (int i=0; i<numdocs; i++){
			buf.write(i + "\t" + doc2numPointers[i] +"\n");
		}
		buf.close();
		input_index.close();

		System.out.println("done!");
	}
}
