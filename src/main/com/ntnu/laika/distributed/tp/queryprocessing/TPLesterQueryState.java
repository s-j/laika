package com.ntnu.laika.distributed.tp.queryprocessing;

import com.ntnu.laika.Constants;
import com.ntnu.laika.distributed.tp.TPQueryBundle;
import com.ntnu.laika.distributed.util.AccumulatorSet;
import com.ntnu.laika.query.processing.ResultHeap;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.FastShortLexicon;
import com.ntnu.laika.structures.lexicon.ShortLexiconEntry;
import com.ntnu.laika.structures.postinglist.DefaultPostingListIterator;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
import com.ntnu.laika.structures.postinglist.PostingListIterator;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TPLesterQueryState implements TPQueryState{
	protected int nextnode, numTerms;
	protected ShortLexiconEntry[] lEntries;
	protected PostingListIterator[] iterators;
	protected boolean keepCandidates;
	protected AccumulatorSet inacc;
	
	protected final int lester_L;
	protected final double avgDocLength;
	protected final boolean skipOpt;
	protected double lester_vt;
	
	private final int maxtf = 2000;
	
	public TPLesterQueryState(TPQueryBundle bundle, Statistics stats, DiskInvertedIndex inv, boolean skipOpt, int lester_L){
		avgDocLength = stats.getAverageDocumentLength();
		this.skipOpt = skipOpt;
		this.lester_L = lester_L;
		nextnode = bundle.getNextNodeID();
		numTerms = bundle.getNumTerms();
		
		inacc = bundle.getAccumulatorSet();
		if (inacc == null) inacc = new AccumulatorSet();
		
		lester_vt = bundle.getLowVal();
		iterators = new PostingListIterator[numTerms];
		lEntries = new ShortLexiconEntry[numTerms];
		
		int termId;
		ShortLexiconEntry _lEntry;
		double maxKF = bundle.getMaximumKeyFrequency();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(bundle.getQueryID());
		for (int j=0; j<numTerms; j++){
			termId = bundle.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(i + " -> " + _lEntry.getN_t());
			lEntries[j] = _lEntry;
			iterators[j] = inv.getPostingListIterator(_lEntry);
			((DefaultPostingListIterator)iterators[j]).prepareForScoring(bundle.getTermQF(j)/maxKF,
					_lEntry.getN_t(), _lEntry.getTF(), numDocs, avgDocLength);
		}
		
		keepCandidates = bundle.getNextNodeID() != 0;
	}
	
	@Override
	public TPResultState processQuery(){
		AccumulatorSet outacc = new AccumulatorSet();
		int doc = 0, remPointers, numPointers, lester_startA, lester_p, lester_ht = 0;
		double score;

		//initialization equivalent to the lines 1-7 of Lester Algorithm 2.
		for (int i = 0; i < numTerms; i++) {
			
			ShortLexiconEntry lEntry = lEntries[i];
			DefaultPostingListIterator iter = (DefaultPostingListIterator)iterators[i];
			
			remPointers = numPointers = lEntry.getN_t();
			
			lester_startA = inacc.size();
			lester_p = (numPointers+lester_L-1)/lester_L;
			
			boolean andmode = false;
			//System.out.println("->" + lester_vt);
			if (lester_L > numPointers){
				//process in OR mode.
				lester_p = numPointers + 1;
			} else if (lester_vt == Double.MIN_VALUE){
				//set lester_ht to be max of first p frequencies
				lester_ht = 0;
				for (int jj=0; jj<lester_p; jj++){
					if (iter.getFrequency() > lester_ht){
						lester_ht = iter.getFrequency();
					}
					iter.next();
				}
				lester_vt = iter.getFakeScore(lester_ht, avgDocLength);
				iter.reset();
			} else  {
				//use the old value of lester_ht and v_t
				//if lester_p is 0, set it to the whole list
				
				if (skipOpt && iter.getFakeScore(maxtf, avgDocLength) < lester_vt){
					lester_ht = maxtf;
					lester_p = numPointers+1;
					andmode = skipOpt;
				} else {
				  	int low = 1, high = maxtf - 1, m = 1;				
					while (high >= low) {
						m = (high + low) >> 1;
						score = iter.getFakeScore(m, avgDocLength);
						if (score > lester_vt) high = m - 1;
						else if (score < lester_vt) low = m + 1;
						else { low = m + 1; break; }
					}
					lester_ht = low;
					if (lester_p <= 1) lester_p = numPointers+1;
				}
			}
		
		//	System.out.println(remPointers+"//");
			if (!andmode){
				//System.out.println("NORMALMODE");
				int lester_s = (lester_ht+1)/2;
				if (lester_s == 0) lester_s = 1;

				for (int k = 0; k < remPointers; k++) {
					//on lester_p-th pointer, recalculate predictors
					//lines 10-16 of Lester Algorithm 2
					if (k==lester_p){
						int accsize = outacc.size() + inacc.size();
						int lester_predict = (int)(accsize + (numPointers-k) * ((double) (accsize-lester_startA) / lester_p));
						int prev_lester_ht = lester_ht;
						if (lester_predict > 1.2*lester_L){
							lester_ht += lester_s;
						} else if (lester_predict < lester_L/1.2){
							lester_ht -= lester_s;
							if (lester_ht < 0) lester_ht = 0;
						}
						if (prev_lester_ht != lester_ht){
							lester_vt = iter.getFakeScore(lester_ht, avgDocLength);
						}
						lester_s = (lester_s+1)/2;
						lester_p = 2*lester_p;
					}
	
					doc = iter.getDocId();
					
					while (inacc.hasMore() && inacc.getDocId() < doc) {
						if (inacc.getScore() >= lester_vt){
							outacc.addLast(inacc.getDocId(), inacc.getScore());
						}
						inacc.next();
					}
						
					if (inacc.hasMore() && inacc.getDocId() == doc){
						score = inacc.getScore() + iter.getScore();
						if (score >= lester_vt) {
							outacc.addLast(doc, score);
						}
						inacc.next();
					} else if (iter.getFrequency() >= lester_ht){
						score = iter.getScore();
						if (score >= lester_vt) {
							outacc.addLast(doc, score);
						}
					}
					iter.next();
				}
				
				iter.close();
				while (inacc.hasMore()){
					if (inacc.getScore() >= lester_vt){
						outacc.addLast(inacc.getDocId(), inacc.getScore());
					}
					inacc.next();
				}
			} else {
				//System.out.println("SKIPMODE");
				int idoc;
				while(inacc.hasMore()){
					doc = inacc.getDocId();
					idoc = iter.getDocId();
					
					if (doc < idoc){
						score = inacc.getScore();
						if (score >= lester_vt) {
							outacc.addLast(inacc.getDocId(), score);
						}	
						inacc.next();	
					} else if (doc == idoc) {
						score = inacc.getScore() + iter.getScore();
						if (score >= lester_vt){
							outacc.addLast(doc, score);
						}
						inacc.next();
					} else { //doc > idoc
						if (!iter.skipTo(doc)) break;
					}
				}
				
				iter.close();
				while (inacc.hasMore()){
					score = inacc.getScore();
					if (score >= lester_vt) {
						outacc.addLast(inacc.getDocId(), score);
					}
					inacc.next();
				}
			}

			inacc.close();
			inacc = outacc; 
			outacc = new AccumulatorSet();
		}
		
		int numberOfResults = inacc.size();
		if (keepCandidates) {
			return new TPResultState(inacc , null, lester_vt, Double.NaN, Double.NaN, numberOfResults);
		} else {
			ResultHeap rheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
			for (int i=0;i<numberOfResults;i++){
				rheap.insertIfGreaterThanLeast(inacc.getDocId(), inacc.getScore());
				inacc.next();
			}
			double min = rheap.minScore();
			double max = rheap.maxScore();
			return new TPResultState(null, rheap, min, max, max, numberOfResults);
		}
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;		
	}
}