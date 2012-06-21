package com.ntnu.laika.distributed.tp;

import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.processing.scoremodels.BM25;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.global.GlobalLexicon;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconEntry;
import com.ntnu.laika.utils.BitVector;
import com.ntnu.laika.utils.Closeable;
import com.ntnu.network.ApplicationHandler;

/** 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPMasterQueryPreprocessing implements Closeable{
	protected final boolean sortByMaxScore;
	
	protected GlobalLexicon lexicon;
	
	public TPMasterQueryPreprocessing(GlobalLexicon lexicon, boolean sortByMaxScore){
		this.lexicon = lexicon;
		this.sortByMaxScore = sortByMaxScore;
		System.out.println("routebyMaxScore: " + sortByMaxScore);
	}
	
	public class MasterQuery {
		public String query;
		public MasterSubQuery[] subQueries;
		public int numNodes;
		public int numTerms;
		public int maxKF = 0;
				
		public MasterQuery(MasterSubQuery[] subQueries, int numNodes, int numTerms, int maxKF, String query){
			this.query = query;
			this.subQueries = subQueries;
			this.numNodes = numNodes;
			this.numTerms = numTerms;
			this.maxKF = maxKF;
		}
		
		public ChannelBuffer toChannelBuffer(int queryid){
			int size = 3 + Constants.INT_SIZE + numNodes * 2 + numTerms * (Constants.INT_SIZE + 1 + Constants.DOUBLE_SIZE); //msgtype+qid, numsubs, <nodeId, <#terms, <termid_int, cnt_byte>>,  
			ChannelBuffer buffer = ChannelBuffers.buffer(size);
			buffer.writeByte(ApplicationHandler.QUERY);
			buffer.writeInt(queryid);
			buffer.writeByte(0xff & maxKF);
			buffer.writeByte(0xff & numNodes);
			//System.out.println("-> q: " + queryid + " " + numNodes + " subs, " + numTerms + " terms");
			int subcnt;
			for ( int i = 0; i < numNodes; i++ ){
				MasterSubQuery sub = subQueries[i];
				subcnt = sub.cnt;
				buffer.writeByte(0xff & sub.nodeid);
				buffer.writeByte(0xff & subcnt);
				for ( int j = 0; j < subcnt; j++ ){
					MasterQueryEntry entry = sub.entries[j];
					//System.out.println(sub.nodeid +": "+entry.lexiconEntry.getTermId() + " " + entry.keyFrequency + " " + entry.maxScore);
					buffer.writeInt(entry.lexiconEntry.getTermId());
					buffer.writeByte(0xff & entry.keyFrequency);
					buffer.writeDouble(entry.maxScore);
				}
			}
			return buffer;
		}
		
		public int getFirstNode(){
			return subQueries[0].nodeid;
		}
	}
	
	public class MasterSubQuery implements Comparable<MasterSubQuery>{
		public int nodeid;
		public MasterQueryEntry[] entries;
		public int cnt;
		public double compvalue; 
		
		public MasterSubQuery(int nodeid, MasterQueryEntry[] entries, int cnt){
			this.nodeid = nodeid;
			this.entries = entries;	
			this.cnt = cnt;
			if (sortByMaxScore) {
				compvalue = entries[0].maxScore;			//highest maxscore
			}	else {
				compvalue = entries[0].lexiconEntry.getTF();//least TF
			}
		}

		@Override
		public int compareTo(MasterSubQuery o){
			if (sortByMaxScore) {							//decreasing highest maxscore
				if (compvalue > o.compvalue) return -1;
				else if (compvalue < o.compvalue) return 1;
				else return 0;
			} else {
				if (compvalue < o.compvalue) return -1;		//increasing least TF
				else if (compvalue > o.compvalue) return 1;
				else return 0;
			}
		}
	}
	
	public class MasterQueryEntry implements Comparable<MasterQueryEntry>{
		public int keyFrequency;
		public double maxScore;
		public GlobalLexiconEntry lexiconEntry;
		
		public MasterQueryEntry(GlobalLexiconEntry lEntry){
			lexiconEntry = lEntry;
			keyFrequency = 1;
			maxScore = Double.NaN;
		}
			
		public MasterQueryEntry(GlobalLexiconEntry lEntry, int kFrequency, double mScore){
			lexiconEntry = lEntry;
			keyFrequency = kFrequency;
			maxScore = mScore;
		}

		public void normalizeMaxScore(int maxKF){
			double kf = (double)keyFrequency / maxKF;
			maxScore *= (BM25.k_3+1d)*kf/(BM25.k_3+kf);
		}
		
		@Override
		public int compareTo(MasterQueryEntry o) {
			if (sortByMaxScore) { //decreasing maxScore
				if (maxScore > o.maxScore) return -1;
				else if (maxScore < o.maxScore) return 1;
				else return 0;
			} else { 			 //increasing TF
				if (lexiconEntry.getTF() < o.lexiconEntry.getTF()) return -1;
				else if (lexiconEntry.getTF() > o.lexiconEntry.getTF()) return 1;
				else return 0;
			}
		}
	}
	
	public MasterQuery processQuery(String querystr){
		String[] terms = querystr.split(" ");
		int numTerms = terms.length;
		if (numTerms == 0) return null;
		
		final int NODES = GlobalLexiconEntry.MAX_NODES_SUPPORTED;

		GlobalLexiconEntry _lEntry = null;
		MasterQueryEntry _mqEntry = null;
		MasterQueryEntry entries[][] = new MasterQueryEntry[NODES][numTerms];
		int counts[] = new int[NODES];
		numTerms = 0;
		int _nodeid, maxKeyFreq = 1;
		
		double maxScore;
		for (String term: terms){
			if (_lEntry != null && _lEntry.getTerm().equals(term)){
				if (++_mqEntry.keyFrequency > maxKeyFreq){
					maxKeyFreq = _mqEntry.keyFrequency;
				}
			} else {
				_lEntry = lexicon.lookup(term);
				if (_lEntry == null) {
					continue;
				} else if ((maxScore = FastMaxScore.getMaxScore(_lEntry.getTermId())) <= 0.0d) {
					_lEntry = null;
					continue;
				} else {
					_mqEntry = new MasterQueryEntry(_lEntry, 1, maxScore);
					numTerms++;
					_nodeid = (new BitVector(_lEntry.getSignature())).getIDs()[0];
					entries[_nodeid][counts[_nodeid]++] = _mqEntry;
				}
			}
		}

		if (numTerms == 0) return null;		
		//create sub-queries, sort entries etc...
		MasterSubQuery[] ret = new MasterSubQuery[NODES];
		int numSubqueries = 0, cnt;
		for (int i=0; i < NODES; i++){
			cnt = counts[i];
			if (cnt > 0){
				for (int j=0; j<cnt; j++) entries[i][j].normalizeMaxScore(maxKeyFreq);
				Arrays.sort(entries[i], 0, cnt);
				ret[numSubqueries++] = new MasterSubQuery(i+1, entries[i], cnt);
			}
		}
		Arrays.sort(ret, 0, numSubqueries);
		return new MasterQuery(ret, numSubqueries, numTerms, maxKeyFreq, querystr);
	}

	@Override
	public void close() {
		lexicon.close();
	}
}