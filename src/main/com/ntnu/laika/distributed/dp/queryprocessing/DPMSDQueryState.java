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
public class DPMSDQueryState implements DPQueryState{
	protected int numTerms;
	protected PostingListIterator[] iterators;
	protected double maxScores[];
	protected double accScores[];
	protected int numReqTerms;
	
	public DPMSDQueryState(DPSubQuery sub, Statistics stats, DiskInvertedIndex inv){
		numTerms = sub.getNumTerms();
		iterators = new PostingListIterator[numTerms];
		accScores = new double[numTerms];
		maxScores = new double[numTerms];

		int termId;		
		ShortLexiconEntry _lEntry;
		double maxKF = sub.getMaximumKeyFrequency();
		double avgDocLength = stats.getAverageDocumentLength();
		double numDocs = stats.getNumberOfDocuments();
		//System.out.println(sub.getQueryID()+"___________________");
		for (int j=0; j<numTerms; j++){
			termId = sub.getTermID(j);
			_lEntry = FastShortLexicon.getLexiconEntry(termId);
			//System.out.println(i + " -> " + _lEntry.getN_t());
			iterators[j] = inv.getPostingListIterator(_lEntry);
			maxScores[j] = sub.getMaxScore(j);
			//System.out.println(j + " ----- " + maxScores[i]);
			((DefaultPostingListIterator)iterators[j]).prepareForScoring(sub.getQF(j)/maxKF,
					sub.getNT(j),sub.getTF(j), numDocs, avgDocLength);
		}
		
		double _accScore = 0.0d;
		for (int i=numTerms-1; i>=0; i--){
			_accScore += maxScores[i];
			accScores[i] = _accScore;
			//System.out.println(i + "-->" + maxScores[i] + " "+ accScores[i]);
		}
		
		numReqTerms = numTerms;
		//System.out.println(hasAccs + " ... " + bundle.getKthBest() + " " + reqScore + "  " + remScore);
	}
	
	@Override	
	public DPResultState processQuery(){
		ResultHeap cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		
		int numberOfResults = 0, canddoc = -1, lastdoc, i, numReqTerms = numTerms;
		double score, reqScore = 0.0d, _decscore;
		
oloop:	while (numReqTerms > 0) {
			//find the least pointer among OR-terms
			lastdoc = canddoc;
			canddoc = Integer.MAX_VALUE;
			for (i = 0; i < numReqTerms; i++){
				if (iterators[i].getDocId() == lastdoc && !iterators[i].next()){
					//close iterator, update scores above, move those from below up
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

			if (cheap.insertIfGreaterThanLeast(canddoc, score)){
				if (reqScore < cheap.minScore()) reqScore = cheap.minScore();

				while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
			}
			
			numberOfResults++;
		}
		
		//System.out.println(reqScore + " / " + cheap.minScore());
		//if (!keepCandidates) System.out.println("====================");
		//else System.out.println("=>" + numberOfResults);
		return new DPResultState(cheap, numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
	}
}