package com.ntnu.laika.distributed.dp;

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
public class DPMasterQueryPreprocessing implements Closeable{
	protected GlobalLexicon lexicon;
	private boolean andmode;
	
	public DPMasterQueryPreprocessing(GlobalLexicon lexicon, boolean andmode){
		this.lexicon = lexicon;
		this.andmode = andmode;
	}
	
	public class MasterQuery {
		public String query;
		public MasterSubQuery[] subQueries;
		public int numNodes;
		public int numTerms;
		public int maxNumNodes;
		public int maxKeyFreq;
		
		public MasterQuery(String query, MasterSubQuery[] subQueries, int numNodes,  int maxNumNodes, int numTerms, int maxKeyFreq){
			this.query = query;
			this.subQueries = subQueries;
			this.numNodes = numNodes;
			this.maxNumNodes = maxNumNodes;
			this.numTerms = numTerms;
			this.maxKeyFreq = maxKeyFreq;
		}
		
		public ChannelBuffer[] toChannelBuffers(int queryid, int mergerID){
			ChannelBuffer ret[] = new ChannelBuffer[maxNumNodes];
			for (int i=0; i<maxNumNodes; i++){
				ret[i] = (subQueries[i].cnt == 0) ? null : subQueries[i].toChannelBuffer(queryid, maxKeyFreq, numNodes, mergerID); 
			}
			return ret;
		}
	}
	
	public class MasterSubQuery{
		public int cnt = 0;
		public MasterQueryEntry[] entries;

		public MasterSubQuery(MasterQueryEntry[] entries){
			this.entries = entries;	
		}

		public ChannelBuffer toChannelBuffer(int queryid, int maxKF, int numNodes, int mergerID){
			int size = 5 + Constants.INT_SIZE + cnt * (Constants.INT_SIZE * 3 + 1 + Constants.DOUBLE_SIZE); //msgtype+qid+maxkf+#terms+<termid_int,kf,maxscore  
			ChannelBuffer buffer = ChannelBuffers.buffer(size);
			buffer.writeByte(ApplicationHandler.QUERY);
			buffer.writeInt(queryid);
			buffer.writeByte(0xff & cnt);
			buffer.writeByte(0xff & maxKF);

			for ( int i = 0; i < cnt; i++ ){
				MasterQueryEntry entry = entries[i];
				buffer.writeInt(entry.lexiconEntry.getTermId());
				buffer.writeInt(entry.lexiconEntry.getN_t());
				buffer.writeInt(entry.lexiconEntry.getTF());
				buffer.writeByte(0xff & entry.keyFrequency);
				buffer.writeDouble(entry.maxScore);
			}
			
			buffer.writeByte(numNodes);
			buffer.writeByte(mergerID);
			return buffer;
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
			if (maxScore > o.maxScore) return -1;
			else if (maxScore < o.maxScore) return 1;
			else return 0;
		}
	}
	
	public MasterQuery processQuery(String querystr){
		String[] terms = querystr.split(" ");
		int numTerms = terms.length;
		if (numTerms == 0) return null;
		
		final int NODES = GlobalLexiconEntry.MAX_NODES_SUPPORTED;
		
		//System.out.println(querystr);
		
		//step 1: look-up ALL terms, clean!
		GlobalLexiconEntry _lEntry = null;
		MasterQueryEntry _mqEntry = null;
		MasterQueryEntry entries[] = new MasterQueryEntry[numTerms];
		numTerms = 0;
		int maxKeyFreq = 1;
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
					entries[numTerms++] = _mqEntry;
				}
			}
		}
				
		//step 2: normalize max-scores and sort...
		for (int i=0; i<numTerms; i++){
			entries[i].normalizeMaxScore(maxKeyFreq);
			//System.out.println(i +": " + entries[i].lexiconEntry.getN_t() + " " + entries[i].lexiconEntry.getTF() + " " + entries[i].keyFrequency + " " + entries[i].maxScore);
		}
		Arrays.sort(entries, 0, numTerms);
		
		//step 3: partition into sub-queries...
		if (numTerms == 0) return null;
		
		//create sub-queries, sort entries etc...
		MasterSubQuery[] ret = new MasterSubQuery[NODES];
		for (int i=0; i<NODES; i++) ret[i] = new MasterSubQuery(new MasterQueryEntry[numTerms]);
		
		int numNodes = 0;
		for (int i=0; i < numTerms; i++){
			for (int nodeid : new BitVector(entries[i].lexiconEntry.getSignature()).getIDs()){
				MasterSubQuery _msq = ret[nodeid];
				_msq.entries[_msq.cnt++] = entries[i];
				if (_msq.cnt == 1) numNodes++;
			}
		}
		
		if (andmode) {	//and optmization: if one of terms is missing - set term count to 0 and decrement the total number of sub-queries
			for (int i=0; i<NODES; i++){
				MasterSubQuery _msq = ret[i];
				if (_msq.cnt > 0 && _msq.cnt < numTerms){	
					_msq.cnt = 0;
					numNodes--;
				}
			}
		}
		
		if (numNodes == 0) return null;
		//System.out.println(numNodes + " nodes " + numTerms + " terms");
		
		return new MasterQuery(querystr, ret, numNodes, NODES, numTerms, maxKeyFreq);
	}

	@Override
	public void close() {
		lexicon.close();
	}
}