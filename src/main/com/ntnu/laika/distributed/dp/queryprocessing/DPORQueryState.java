package com.ntnu.laika.distributed.dp.queryprocessing;

import com.ntnu.laika.Constants;
import com.ntnu.laika.distributed.dp.DPSubQuery;
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
public class DPORQueryState implements DPQueryState{
	protected int numTerms;
	protected PostingListIterator[] iterators;
	
	//private int uid = (int)(Math.random() * 10000);
	
	public DPORQueryState(DPSubQuery sub, Statistics stats, DiskInvertedIndex inv){
		numTerms = sub.getNumTerms();
		iterators = new PostingListIterator[numTerms];
		int termId;	
		ShortLexiconEntry _lEntry;
		double maxKF = sub.getMaximumKeyFrequency();
		double avgDocLength = stats.getAverageDocumentLength();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(uid + " " + sub.getQueryID() + " " + maxKF);
		for (int j=0; j<numTerms; j++){
			termId = sub.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(uid + " " + j + ": " + _lEntry.getN_t() + " " + _lEntry.getTF() + " " + sub.getQF(j) + " " + sub.getNT(j) + " " + sub.getTF(j));
			iterators[j] = inv.getPostingListIterator(_lEntry);
			((DefaultPostingListIterator)iterators[j]).prepareForScoring(sub.getQF(j)/maxKF,
					sub.getNT(j),sub.getTF(j), numDocs, avgDocLength);
		}
	}
	
	public DPResultState processQuery(){
		ResultHeap cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		
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
			cheap.insertIfGreaterThanLeast(canddoc, score);
			
			if (minScore > score) minScore = score;
			else if (maxScore < score) maxScore = score;
			
			numberOfResults++;
		}
		
		return new DPResultState(cheap, numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
	}
}