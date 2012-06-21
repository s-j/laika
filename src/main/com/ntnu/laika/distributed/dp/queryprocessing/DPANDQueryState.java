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
public class DPANDQueryState implements DPQueryState{
	protected int nextnode, numTerms;
	protected PostingListIterator[] iterators;
	protected double minScore = Double.MAX_VALUE;
	protected double maxScore = Double.MIN_VALUE;
	
	public DPANDQueryState(DPSubQuery sub, Statistics stats, DiskInvertedIndex inv){
		numTerms = sub.getNumTerms();
		iterators = new PostingListIterator[numTerms];

		int termId;
		ShortLexiconEntry _lEntry;
		double maxKF = sub.getMaximumKeyFrequency();
		double avgDocLength = stats.getAverageDocumentLength();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(bundle.getQueryID());
		for (int j=0; j<numTerms; j++){
			termId = sub.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(i + " -> " + _lEntry.getN_t());
			iterators[j] = inv.getPostingListIterator(_lEntry);
			((DefaultPostingListIterator)iterators[j]).prepareForScoring(sub.getQF(j)/maxKF,
					sub.getNT(j),sub.getTF(j), numDocs, avgDocLength);
		}
	}
	
	@Override
	public DPResultState processQuery(){
		ResultHeap cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		
		int numberOfResults = 0, doc = 0, _doc, i;
		
		double score;
		//long time, istart, itime = 0l;
		//time = System.currentTimeMillis();
loop: while (true) {
			doc = iterators[0].getDocId();
			
			for (i=1; i<numTerms; i++){
				if (!iterators[i].skipTo(doc)) break loop;
				
				_doc = iterators[i].getDocId();
				if (_doc > doc) {
					if (iterators[0].skipTo(_doc)) continue loop;
					else break loop;
				}
			}
			
			//istart = System.currentTimeMillis();
			numberOfResults++;
			score = 0.0d;
			for (i=0; i<numTerms; i++) score += iterators[i].getScore();
			//itime += System.currentTimeMillis() - istart; 
			
			cheap.insertIfGreaterThanLeast(doc, score);
				
			if (minScore > score) minScore = score;
			else if (maxScore < score) maxScore = score;
			if (!iterators[0].next()) break loop;
		}

		//time = System.currentTimeMillis() - time;
		//System.out.println(time + " / " + itime);
		return new DPResultState(cheap, numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;		
	}
}