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
public class TPMSDANDQueryState implements TPQueryState{
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
	
	public TPMSDANDQueryState(TPQueryBundle bundle, Statistics stats, DiskInvertedIndex inv){
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
		//System.out.println(bundle.getQueryID());
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
		//System.out.println("====================");
		if (hasAccs) numTerms++;
		double _accScore = 0.0d;
		for (i=numTerms-1; i>=0; i--){
			_accScore += maxScores[i];
			accScores[i] = _accScore;
			//System.out.println(i + "-->" + maxScores[i] + " "+ accScores[i]);
		}
		//System.out.println(hasAccs + " ... " + reqScore + "  " + remScore + " " + accScores[hasAccs?1:0]);

		numReqTerms = numTerms;
		
		if (hasAccs) {
			reqScore = bundle.getKthBest() - remScore;
			while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
		}		
		keepCandidates = bundle.getNextNodeID() != 0;
	}
	
	@Override	
	public TPResultState processQuery(){
		//double origReqScore = reqScore;
		ResultHeap cheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
		AccumulatorSet outacc = keepCandidates ? new AccumulatorSet() : null; 
		
		int numberOfResults = 0;
		int doc, _doc, i; 
		double score, _reqScore;

		//double extra = 0.75;
loop:	while (true){
			doc = iterators[0].getDocId();
			score = iterators[0].getScore();
						
			for (i = 1; i < numTerms; i++){
				if (score + accScores[i] < reqScore) break;
				
				if (!iterators[i].skipTo(doc)) break loop;
				
				_doc = iterators[i].getDocId();
				if (_doc > doc){
					if (iterators[0].skipTo(_doc)) continue loop;
					else break loop;
				}
				
				score += iterators[i].getScore();
			}
			
			if (i==numTerms && score >= reqScore){
				if (keepCandidates) {
					outacc.addLast(doc, score);
				}

				if (cheap.insertIfGreaterThanLeast(doc, score)){
					_reqScore = cheap.minScore() - remScore;
					if (reqScore < _reqScore){
						reqScore = _reqScore;
					}
				}
				numberOfResults++;
			}
			
			if (!iterators[0].next()) break loop;
		}
			
		for (i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
		/*additional cleaning
		if (keepCandidates){
			_reqScore = cheap.minScore() - remScore;
			if (reqScore < _reqScore) reqScore = _reqScore;
				
			if (reqScore > origReqScore){
				AccumulatorSet inacc = outacc;
				outacc = new AccumulatorSet();
				while (inacc.hasMore()){
					score = inacc.getScore(); 
					if (score >= reqScore) outacc.addLast(inacc.getDocId(), score);
					inacc.next();
				}
				inacc.close();
				numberOfResults = outacc.size();
			}
		}*/
		
		return new TPResultState(outacc, cheap, reqScore, cheap.maxScore(), cheap.minScore(), numberOfResults);
	}
	
	public void close() {
		for (int i=0; i<numTerms; i++) iterators[i].close();
		numTerms = 0;
	}
}