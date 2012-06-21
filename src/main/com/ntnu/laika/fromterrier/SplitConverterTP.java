package com.ntnu.laika.fromterrier;

import java.io.IOException;
import java.util.Properties;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.LocalIndex;
import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.ShortLexiconOutputStream;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconEntry;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconOutputStream;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;
import com.ntnu.laika.structures.postinglist.PostingListOutputStream;
import com.ntnu.laika.utils.BitVector;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class SplitConverterTP {

	private class IndexWrapper{
		protected com.ntnu.laika.structures.LocalIndex index;
		protected PostingListOutputStream plos;
		protected ShortLexiconOutputStream los;
		
		private BitVector docs;
		
		public IndexWrapper(String path, int maxnumdocs){
			index = new LocalIndex(path);
			plos = index.getPostingListOutputStream();
			los = index.getShortLexiconOutputStream();
			docs = new BitVector(maxnumdocs);
		}
	
		public void nextEntry(String term, int id, int nt, int tf, int[][] scores){
			for (int i=0; i<scores[0].length; i++)
				docs.setBit(scores[0][i]);
			
			LexiconEntry lEntry = plos.nextEntry(term, nt, tf, scores);
			los.nextEntry(id, nt, tf, lEntry.getEndOffset());
		}
		
		public void saveAndClose(Statistics globalStats, int id, int workerscnt){
			index.setStatistics(globalStats);
			index.setLocalStatistics(new Statistics(docs.getBitCount(),
					los.getNumberOfTerms(),los.getNumberOfPointers(), los.getNumberOfTokens()));

			Properties props = index.getIndexProperties();
			props.setProperty("use_skips", Constants.USE_SKIPS+"");
			props.setProperty("id", id+"");
			props.setProperty("workerscnt", workerscnt+"");
			
			plos.close();
			los.close();
			index.close();			
		}
	}
	
	public SplitConverterTP(String src, String mainpath, int numnodes) throws IOException, InterruptedException{
		Index input_index = new com.ntnu.laika.structures.Index(src);
		Statistics stats = input_index.getStatistics();
		int numterms = stats.getNumberOfUniqueTerms();
		PostingListInputStream plis = input_index.getPostingListInputStream(numterms); 
		
		//Constants.USE_SKIPS = true;
		Runtime.getRuntime().exec("mkdir " + mainpath).waitFor();
		Runtime.getRuntime().exec("mkdir " + mainpath + "0/").waitFor();
		
		MasterIndex mindex = new MasterIndex(mainpath+"0/");
		GlobalLexiconOutputStream glos = mindex.getGlobalLexiconOutputStream();
		
		MSDPartitioning.partitionBySize(input_index, numnodes);
		
		IndexWrapper wrappers[] = new IndexWrapper[8];
		for (int i=1; i<=numnodes; i++){
			Runtime.getRuntime().exec("mkdir " + mainpath + i + "/").waitFor();
			wrappers[i-1] = new IndexWrapper(mainpath + i, stats.getNumberOfDocuments());
		}
		
		for (int i=0; i<numterms; i++){
			LexiconEntry _lE = plis.nextEntry();
			String term = _lE.getTerm();
			int nt = _lE.getN_t();
			int tf = _lE.getTF();
			
			int[][] scores = new int[2][nt];
			for (int k=0; k<nt; k++){
				scores[0][k] = plis.getDocId();
				scores[1][k] = plis.getFrequency();
				plis.next();
			}
			
			int destnode = MSDPartitioning.getNodeID(_lE.getTermId());
			//int destnode = (term.hashCode() % numnodes);
			if (destnode < 0) destnode += numnodes;
			wrappers[destnode].nextEntry(term, i, nt, tf, scores);	
			
			//write  global mapping
			glos.nextEntry(term, i, nt, tf, (new BitVector(GlobalLexiconEntry.MAX_NODES_SUPPORTED, destnode)).data);
	
			if (i % 1000000 == 0) System.out.println((100 * i)/numterms + "%");
		}
		plis.close();
		input_index.close();
		glos.close();
		
		
		Statistics global = new Statistics(
				stats.getNumberOfDocuments(),
				stats.getNumberOfUniqueTerms(),
				stats.getNumberOfPointers(),
				stats.getNumberOfTokens());
		
		for (int i=0; i<numnodes; i++)
				wrappers[i].saveAndClose(global, i+1, numnodes);

		Properties props = mindex.getIndexProperties();
		
		props.setProperty("use_skips", Constants.USE_SKIPS+"");
		props.setProperty("id", "0");
		props.setProperty("workerscnt", numnodes+"");
		mindex.setStatistics(global);
		mindex.close();
	}
	
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String idxsrc = "/home/simonj/index4/";
		String idxdst = "/home/simonj/index4-dist-S/";
		int numnodes = 8;
		
		//idx, lex and properties
		System.out.println("splitting index: creating subdirectories, inverted and lexicon files");
		new SplitConverterTP(idxsrc, idxdst, 8);
		
		//queries
		//System.out.println("copying query sets");
		//Runtime.getRuntime().exec("cp " + idxsrc + "qrels.tb06.top50 " + idxdst + "0/").waitFor();
		//Runtime.getRuntime().exec("cp " + idxsrc + "queries801-850 " + idxdst + "0/").waitFor();
		//Runtime.getRuntime().exec("cp " + idxsrc + "querylog.test " + idxdst + "0/").waitFor();
		//doc
		System.out.println("copying the main document dictionary");
		Runtime.getRuntime().exec("cp " + idxsrc + "index.doc " + idxdst + "0/").waitFor();
		System.out.println("creating support document dictionaries");
		DocDictConverter.docDict2Short(idxsrc, idxdst+"1/");
		System.out.println(100/numnodes + "%");
		for (int i=2; i<=numnodes;i++){
			System.out.println("cp " + idxdst + "1/index.sdoc " + idxdst + i + "/");
			Runtime.getRuntime().exec("cp " + idxdst + "1/index.sdoc " + idxdst + i + "/").waitFor();
			System.out.println((i*100)/numnodes + "%");	
		}
		
		//fms
		System.out.println("copying fast max scores");
		Runtime.getRuntime().exec("cp " + idxsrc + "index.fms " + idxdst + "0/").waitFor();
		
		System.out.println("Done! =)");
	}
}
