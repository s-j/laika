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
public class TPMSDQueryState implements TPQueryState{
	protected int nextnode, numTerms;
	protected PostingListIterator[] iterators;
	protected double maxScores[];
	protected double accScores[];
	protected boolean keepCandidates;
	protected boolean hasAccs;
	protected double maxScore;
	protected double reqScore = 0.0d;
	protected double remScore;
	protected int numReqTerms;
	
	public TPMSDQueryState(TPQueryBundle bundle, Statistics stats, DiskInvertedIndex inv){
		nextnode = bundle.getNextNodeID();
		numTerms = bundle.getNumTerms();
		remScore = bundle.getRemScore();
		maxScore = bundle.getHighVal();
		
		AccumulatorSet inacc = bundle.getAccumulatorSet();
		
		int i=0;
		if (inacc != null){
			hasAccs = true;
			iterators = new PostingListIterator[numTerms + 1];
			accScores = new double[numTerms+1];
			maxScores = new double[numTerms+1];
			iterators[0] = inacc;
			maxScores[0] = bundle.getHighVal();
			i=1;
		} else {
			hasAccs = false;
			iterators = new PostingListIterator[numTerms];
			accScores = new double[numTerms];
			maxScores = new double[numTerms];
		}

		int termId;
		
		ShortLexiconEntry _lEntry;
		double maxKF = bundle.getMaximumKeyFrequency();
		double avgDocLength = stats.getAverageDocumentLength();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(bundle.getQueryID()+"___________________");
		for (int j=0; j<numTerms; j++){
			termId = bundle.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(i + " -> " + _lEntry.getN_t());
			iterators[i] = inv.getPostingListIterator(_lEntry);
			maxScores[i] = bundle.getMaxScore(j);
			//System.out.println(j + " ----- " + maxScores[i]);
			((DefaultPostingListIterator)iterators[i]).prepareForScoring(bundle.getTermQF(j)/maxKF,
					_lEntry.getN_t(), _lEntry.getTF(), numDocs, avgDocLength);
			i++;
		}
		if (hasAccs) numTerms++;
		double _accScore = 0.0d;
		for (i=numTerms-1; i>=0; i--){
			_accScore += maxScores[i];
			accScores[i] = _accScore;
			//System.out.println(i + "-->" + maxScores[i] + " "+ accScores[i]);
		}
		
		numReqTerms = numTerms;
		if (hasAccs) {
			reqScore = bundle.getKthBest() - remScore;
			if (reqScore < 0) reqScore = 0.0d;
			while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
		}
		//System.out.println(hasAccs + " ... " + bundle.getKthBest() + " " + reqScore + "  " + remScore);
		keepCandidates = bundle.getNextNodeID() != 0;
	}
	
	@Override	
	public TPResultState processQuery(){
		double origReqScore = reqScore;
		ResultHeap cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		AccumulatorSet outacc = keepCandidates ? new AccumulatorSet() : null; 
		
		int numberOfResults = 0, canddoc = -1, lastdoc, i, numReqTerms = numTerms;
		double score, _reqScore, _decscore;
	
oloop:	while (numReqTerms > 0) {
			//find the least pointer among OR-terms
			lastdoc = canddoc;
			canddoc = Integer.MAX_VALUE;
			for (i = 0; i < numReqTerms; i++){
				if (iterators[i].getDocId() == lastdoc && !iterators[i].next()){
					//close iterator, update scores above, move those from below up
					if (hasAccs && i==0) hasAccs = false;
					iterators[i].close();
					_decscore = maxScores[i];
					for (int j=0; j<i; j++) accScores[j] -= _decscore;

					numTerms--;
					for (int j=i; j<numTerms; j++){
						iterators[j] = iterators[j+1];
						maxScores[j] = maxScores[j+1];
						accScores[j] = accScores[j+1];
					}
					i--;

					//if no OrTerms right now - suspend processing!
					numReqTerms--;
					while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
					if (numReqTerms <= 0) break oloop;
				} else if (iterators[i].getDocId() < canddoc){
					canddoc = iterators[i].getDocId();
				}
			}
			
			//accumulate corresponding postings. Be ready to prune half-evaluated
			score = 0.0d;				
			for (i = 0; i < numTerms && score + accScores[i] >= reqScore; i++){
				if (i>=numReqTerms && !iterators[i].skipTo(canddoc)){	//AND terms
					//remove term iterator
					iterators[i].close();
					_decscore = maxScores[i];
					for (int j=0; j < i; j++) accScores[j] -= _decscore;
					//move remaining iterators
					numTerms--;
					for (int j=i; j<numTerms; j++){
						iterators[j] = iterators[j+1];
						accScores[j] = accScores[j+1];
						maxScores[j] = maxScores[j+1];
					}				
					i--;
				} else if (iterators[i].getDocId() == canddoc) {
					score += iterators[i].getScore();
				}
			}

			if (score <= reqScore) continue;
			
			//if reached here -> add result, update tags etc.
			if (keepCandidates)	outacc.addLast(canddoc, score);

			if (cheap.insertIfGreaterThanLeast(canddoc, score)){
				_reqScore = cheap.minScore() - remScore;
				if (reqScore < _reqScore) reqScore = _reqScore;

				while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
			}
			
			numberOfResults++;
		}
		
		//additional cleaning
		if (keepCandidates){
			_reqScore = cheap.minScore() - remScore;
			if (reqScore < _reqScore) reqScore = _reqScore;
				
			if (reqScore > origReqScore){
				//System.out.println("cleaing from " + numberOfResults);
				AccumulatorSet inacc = outacc;
				outacc = new AccumulatorSet();
				while (inacc.hasMore()){
					score = inacc.getScore(); 
					if (score > reqScore) outacc.addLast(inacc.getDocId(), score);
					inacc.next();
				}
				inacc.close();
				numberOfResults = outacc.size();
			}
		}
		
		//System.out.println(reqScore + " / " + cheap.minScore());
		//if (!keepCandidates) System.out.println("====================");
		//else System.out.println("=>" + numberOfResults);
		return new TPResultState(outacc, cheap, reqScore, cheap.maxScore(), cheap.minScore(), numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
	}
}