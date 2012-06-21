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
public class TPORQueryState implements TPQueryState{
	protected int nextnode, numTerms;
	protected PostingListIterator[] iterators;
	protected boolean keepCandidates;
	protected boolean hasAccs;
	
	public TPORQueryState(TPQueryBundle bundle, Statistics stats, DiskInvertedIndex inv){
		nextnode = bundle.getNextNodeID();
		numTerms = bundle.getNumTerms();
		
		AccumulatorSet inacc = bundle.getAccumulatorSet();	
		int i=0;
		if (inacc != null){
			hasAccs = true;
			iterators = new PostingListIterator[numTerms + 1];
			iterators[0] = inacc;
			i=1;
		} else {
			hasAccs = false;
			iterators = new PostingListIterator[numTerms];
		}

		int termId;
		
		ShortLexiconEntry _lEntry;
		double maxKF = bundle.getMaximumKeyFrequency();
		double avgDocLength = stats.getAverageDocumentLength();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(bundle.getQueryID());
		for (int j=0; j<numTerms; j++){
			termId = bundle.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(i + " -> " + _lEntry.getN_t());
			iterators[i] = inv.getPostingListIterator(_lEntry);
			((DefaultPostingListIterator)iterators[i]).prepareForScoring(bundle.getTermQF(j)/maxKF,
					_lEntry.getN_t(), _lEntry.getTF(), numDocs, avgDocLength);
			i++;
		}
		
		if (hasAccs) numTerms++;
		keepCandidates = bundle.getNextNodeID() != 0;	
	}
	
	public TPResultState processQuery(){
		ResultHeap cheap = null;
		AccumulatorSet outacc = null; 

		if (keepCandidates) outacc = new AccumulatorSet(); 
		else cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		
		int numberOfResults = 0, canddoc, i;
		double score, minScore = Double.MAX_VALUE, maxScore = Double.MIN_VALUE;
	
		while (numTerms > 0) {
			//find the least pointer among OR-terms
			canddoc = iterators[0].getDocId();
			for (i = 1; i < numTerms; i++) 
				if (canddoc > iterators[i].getDocId())
					canddoc = iterators[i].getDocId();
			
			//accumulate corresponding postings, increment iterators, remove any posting lists that end up...
			score = 0.0d;				
			for (i = 0; i < numTerms; i++){										
				if (iterators[i].getDocId() == canddoc){ 
					score += iterators[i].getScore();
					if (!iterators[i].next()){
						//close iterator, update scores above, move those from below up
						iterators[i].close();
						numTerms--;
						for (int j=i; j<numTerms; j++) iterators[j] = iterators[j+1];
						i--;
					}
				}
			}
			
			//if reached here -> add result, update tags etc.
			if (keepCandidates) outacc.addLast(canddoc, score);
			else cheap.insertIfGreaterThanLeast(canddoc, score);
			
			if (minScore > score) minScore = score;
			else if (maxScore < score) maxScore = score;
			
			numberOfResults++;
		}
		
		return new TPResultState(outacc, cheap, minScore, maxScore, Double.NaN, numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
	}
}