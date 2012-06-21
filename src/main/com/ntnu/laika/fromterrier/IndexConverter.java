package com.ntnu.laika.fromterrier;

import java.io.IOException;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.LexiconOutputStream;
import com.ntnu.laika.structures.postinglist.PostingListOutputStream;

import uk.ac.gla.terrier.structures.Index;
import uk.ac.gla.terrier.structures.InvertedIndexInputStream;
import uk.ac.gla.terrier.structures.LexiconInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class IndexConverter {
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/home/simonj/workstuff/Java/terrier";
		String outpath ="/home/simonj/terrier";
		
		if (args.length>0){
			inpath = args[0];
			outpath= args[1];
		}
		System.setProperty("terrier.home", inpath);
		System.setProperty("terrier.etc",  inpath + "/etc");
		System.setProperty("terrier.setup", inpath + "/etc/terrier.properties");
		
		uk.ac.gla.terrier.structures.Index tIndex = Index.createIndex();
		LexiconInputStream lis = (LexiconInputStream)tIndex.getIndexStructureInputStream("lexicon");
		InvertedIndexInputStream iis = (InvertedIndexInputStream) tIndex.getIndexStructureInputStream("inverted");

		com.ntnu.laika.structures.Index lIndex = new com.ntnu.laika.structures.Index(outpath);
		Constants.USE_SKIPS = true;
		Constants.DOCNO_BYTE_LENGTH = 8;
		Constants.STRING_BYTE_LENGTH= 40;
		Constants.MAX_NUMBER_OF_DOCUMENTS = tIndex.getCollectionStatistics().getNumberOfDocuments();
		
		PostingListOutputStream plow = lIndex.getPostingListOutputStream();
		LexiconOutputStream low = lIndex.getLexiconOutputStream();
		Statistics stats = lIndex.getStatistics();
		
		int i = 0;
		
		while (lis.readNextEntry() > -1){
			String term = lis.getTerm();
			int nt = lis.getNt();
			int tf = lis.getTF();
			int[][] scores = iis.getNextDocuments();
			LexiconEntry lEntry = plow.nextEntry(term, nt, tf, scores);
			i++;
			//System.out.println(i + " " + lEntry.getTermId() + " " + lEntry.getN_t() + "  " +
			//lEntry.getTF() + " " + lEntry.getStartOffset() + " " + lEntry.getEndOffset());
			if (i%1000000 == 0) System.out.println(i);
			low.nextEntry(lEntry);
		}
		
		
		stats.setNumberOfDocuments(tIndex.getCollectionStatistics().getNumberOfDocuments());
		stats.setNumberOfTokens(low.getNumberOfTokens());
		stats.setNumberOfPointers(low.getNumberOfPointers());
		stats.setNumberOfUniqueTerms(low.getNumberOfTerms());
		
		plow.close();
		low.close();
		lIndex.close();
		
		lis.close();
		iis.close();
		tIndex.close();
	}
}
