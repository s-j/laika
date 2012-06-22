package com.ntnu.laika.distributed.tp;

import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.ntnu.laika.Constants;
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
public class TPMasterQueryPreprocessingHPQP implements Closeable{

	protected GlobalLexicon lexicon;	
    protected static boolean isGLB = true;
	protected long loads[];
	
	public TPMasterQueryPreprocessingHPQP(GlobalLexicon lexicon){
		System.out.println("use GLB: " + isGLB);
		this.lexicon = lexicon;
		loads = new long[GlobalLexiconEntry.MAX_NODES_SUPPORTED];
	}
	
	public class MasterQuery {
		public MasterSubQuery[] subQueries;
		public int numNodes;
		public int numTerms;
		public int maxKF = 0;
				
		public MasterQuery(MasterSubQuery[] subQueries, int numNodes, int numTerms, int maxKF){
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
		public int compvalue; 
		
		public MasterSubQuery(int nodeid, MasterQueryEntry[] entries, int cnt){
			this.nodeid = nodeid;
			this.entries = entries;	
			this.cnt = cnt;
			this.compvalue = entries[cnt-1].lexiconEntry.getN_t();
		}

		@Override
		public int compareTo(MasterSubQuery o){
			if (compvalue < o.compvalue) return -1;
			else if (compvalue > o.compvalue) return 1;
			else return 0;
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

		@Override
		public int compareTo(MasterQueryEntry o) {
			int nt = lexiconEntry.getN_t();
			int ont = o.lexiconEntry.getN_t();
			if (nt < ont) return -1;
			else if (nt > ont) return 1;
			else return 0;
		}
	}
	
	public void decrementLoads(){
		long minvalue = Long.MAX_VALUE;
		for (long load : loads) if (minvalue > load) minvalue = load;
		for (int i=0; i<loads.length; i++) loads[i] -= minvalue;
	}
	
	public int leastLoadedNode(){
		int bestcandidate = 0;
		for (int i=1; i<loads.length; i++) if (loads[bestcandidate] > loads[i]) bestcandidate =i;
		return bestcandidate;
	}

	public int leastLoadedNode(boolean[] nmask){
		int bestcandidate = -1;
		for (int i=0; i<nmask.length; i++)
			if (nmask[i]){
				bestcandidate=i;
				break;
			}
	
		if (bestcandidate > -1) {				//check remaining
			for (int i = bestcandidate + 1; i < nmask.length; i++)
				if (nmask[i] && loads[bestcandidate] > loads[i])
					bestcandidate = i;
		} else {
			bestcandidate = 0;//all are false
			for (int i = 1; i < loads.length; i++)
				if (loads[bestcandidate] > loads[i])
					bestcandidate = i;
				
		}
		return bestcandidate;
	}

	public MasterQuery processQuery(String querystr){
		String[] terms = querystr.split(" ");
		int numTerms = terms.length;
		if (numTerms == 0) return null;
		
		final int NODES = GlobalLexiconEntry.MAX_NODES_SUPPORTED;
		//take all normal terms, and leave replicated terms
		GlobalLexiconEntry _lEntry = null;
		MasterQueryEntry _mqEntry = null;
		MasterQueryEntry entries[][] = new MasterQueryEntry[NODES][numTerms];
		int counts[] = new int[NODES];
		MasterQueryEntry replicatedEntries[] = new MasterQueryEntry[numTerms];
		numTerms = 0;
		int numReplicatedTerms = 0, _nodeids[], _nodeid, maxKeyFreq = 0;
		boolean nmask[] = new boolean[NODES];
		
		for (String term: terms){
			if (_lEntry != null && _lEntry.getTerm().equals(term)){
				if (++_mqEntry.keyFrequency > maxKeyFreq) maxKeyFreq = _mqEntry.keyFrequency;
			} else {
				_lEntry = lexicon.lookup(term);
				
				if (_lEntry == null) continue;

				//System.out.println(_lEntry.toString());
				_mqEntry = new MasterQueryEntry(_lEntry, 1, FastMaxScore.getMaxScore(_lEntry.getTermId()));
				
				numTerms++;
				_nodeids = (new BitVector(_lEntry.getSignature())).getIDs();

				if (_nodeids.length == 1){
					_nodeid = _nodeids[0];
					entries[_nodeid][counts[_nodeid]++] = _mqEntry;
					loads[_nodeid] += _lEntry.getN_t();
					nmask[_nodeid] = true;
				} else {
					replicatedEntries[numReplicatedTerms++] = _mqEntry;
				}
			}
		}
		
		if (numTerms == 0) return null;
		else if (maxKeyFreq == 0) maxKeyFreq = 1;
		
		//deal with replicated terms...
		for (int i=0; i<numReplicatedTerms; i++){
			_mqEntry = replicatedEntries[i];
			//_nodeids = (new BitVector(_mqEntry.lexiconEntry.getSignature())).getIDs();
			_nodeid = isGLB ? leastLoadedNode() : leastLoadedNode(nmask);
			//printLoads();
			//System.out.println(_mqEntry.lexiconEntry.getTerm() +"->" +_nodeid);
			entries[_nodeid][counts[_nodeid]++] = _mqEntry;
			loads[_nodeid] += _mqEntry.lexiconEntry.getN_t();
			nmask[_nodeid] = true;
		}
		
		//create sub-queries, sort entries etc...
		MasterSubQuery[] ret = new MasterSubQuery[NODES];
		int numSubqueries = 0, cnt;
		for (int i=0; i < NODES; i++){
			cnt = counts[i];
			if (cnt > 0){			
				Arrays.sort(entries[i], 0, cnt);
				ret[numSubqueries++] = new MasterSubQuery(i+1, entries[i], cnt);
			}
		}
		Arrays.sort(ret, 0, numSubqueries);
		return new MasterQuery(ret, numSubqueries, numTerms, maxKeyFreq);
	}

	protected void printLoads() {
		for (int i=0; i<loads.length; i++) System.out.print(i+": "+ loads[i]+"\t");
		System.out.println();
	}

	@Override
	public void close() {
		lexicon.close();
	}
}