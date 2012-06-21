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
public class TPANDQueryState implements TPQueryState{
	protected int nextnode, numTerms;
	protected PostingListIterator[] iterators;
	protected boolean keepCandidates;
	protected boolean hasAccs;
	protected double minScore = Double.MAX_VALUE;
	protected double maxScore = Double.MIN_VALUE;
	
	public TPANDQueryState(TPQueryBundle bundle, Statistics stats, DiskInvertedIndex inv){
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
	
	@Override
	public TPResultState processQuery(){
		ResultHeap cheap = null;
		AccumulatorSet outacc = null; 

		if (keepCandidates) outacc = new AccumulatorSet(); 
		else cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		
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
			
			if (keepCandidates)	outacc.addLast(doc, score);
			else cheap.insertIfGreaterThanLeast(doc, score);
				
			if (minScore > score) minScore = score;
			else if (maxScore < score) maxScore = score;
			if (!iterators[0].next()) break loop;
		}
		if (!keepCandidates) minScore = cheap.minScore();
		//time = System.currentTimeMillis() - time;
		//System.out.println(time + " / " + itime);
		return new TPResultState(outacc, cheap, minScore, maxScore, Double.NaN, numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;		
	}
}