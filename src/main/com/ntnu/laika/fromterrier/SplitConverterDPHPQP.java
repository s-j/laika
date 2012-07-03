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
public class SplitConverterDPHPQP {

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
	
	public SplitConverterDPHPQP(String mappath, String src, String mainpath, int numnodes) throws IOException, InterruptedException{
		DPDocMapping docmap = new DPDocMapping(mappath, numnodes);
		Index input_index = new com.ntnu.laika.structures.Index(src);
		Statistics stats = input_index.getStatistics();
		int numterms = stats.getNumberOfUniqueTerms();
		PostingListInputStream plis = input_index.getPostingListInputStream(numterms); 
		
		//Constants.USE_SKIPS = true;
		Runtime.getRuntime().exec("mkdir " + mainpath).waitFor();
		Runtime.getRuntime().exec("mkdir " + mainpath + "0/").waitFor();
		
		MasterIndex mindex = new MasterIndex(mainpath+"0/");
		GlobalLexiconOutputStream glos = mindex.getGlobalLexiconOutputStream();
				
		IndexWrapper wrappers[] = new IndexWrapper[numnodes];
		for (int i=1; i<=numnodes; i++){
			Runtime.getRuntime().exec("mkdir " + mainpath + i + "/").waitFor();
			wrappers[i-1] = new IndexWrapper(mainpath + i, stats.getNumberOfDocuments());
		}
		
		for (int i=0; i<numterms; i++){
			LexiconEntry _lE = plis.nextEntry();
			String term = _lE.getTerm();
			int nt = _lE.getN_t();
			int tf = _lE.getTF();
			
			//PARTITIONING START
			//partition postings...
			int[][][] scores = new int[numnodes][2][nt];
			int[][] counts = new int[numnodes][2];
			int docid, freq, destnode;
			for (int k=0; k<nt; k++){
				docid = plis.getDocId(); freq = plis.getFrequency();
				
				//destnode = docid % numnodes;
				destnode = docmap.getNode(docid);
				scores[destnode][0][counts[destnode][0]] = docid;
				scores[destnode][1][counts[destnode][0]] = freq;
				counts[destnode][0]++; 
				counts[destnode][1]+=freq;
				
				plis.next();
			}
			
			//write lists and calculate the signature...
			BitVector bv = new BitVector(GlobalLexiconEntry.MAX_NODES_SUPPORTED);
			for (int k=0; k<numnodes; k++){
				int _nt = counts[k][0];
				if (_nt > 0) {
					int _tf = counts[k][1];
					wrappers[k].nextEntry(term, i, _nt, _tf, scores[k]);
					bv.setBit(k);
				}
			}
			
			//write the global mapping
			glos.nextEntry(term, i, nt, tf, bv.data);
			
			//PARTITIONING END
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
		
		//props.setProperty("use_skips", Constants.USE_SKIPS+"");
		props.setProperty("id", "0");
		props.setProperty("workerscnt", numnodes+"");
		mindex.setStatistics(global);
		mindex.close();
	}
	
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String docmap = "/home/simonj/workstuff/2012/Enver/partvec_doc/partvec_doc.4.HP";
		String idxsrc = "/mnt/data/data/laika_v2/index/";
		String idxdst = "/mnt/data/data/ENVERIDX/4.DPHP.RNO/";
		int numnodes = 4;
		
		//idx, lex and properties
		Runtime.getRuntime().exec("mkdir " + idxdst).waitFor();
		System.out.println("splitting index: creating subdirectories, inverted and lexicon files");
		new SplitConverterDPHPQP(docmap, idxsrc, idxdst, numnodes);
		
		//queries
		System.out.println("copying query sets");
		//Runtime.getRuntime().exec("cp " + idxsrc + "qrels.tb06.top50 " + idxdst + "0/").waitFor();
		//Runtime.getRuntime().exec("cp " + idxsrc + "queries801-850 " + idxdst + "0/").waitFor();
		//Runtime.getRuntime().exec("cp " + idxsrc + "querylog.test " + idxdst + "0/").waitFor();
		//doc
		Runtime.getRuntime().exec("cp /mnt/data/data/ENVERIDX/querylog.test " + idxdst + "0/").waitFor();
		
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
