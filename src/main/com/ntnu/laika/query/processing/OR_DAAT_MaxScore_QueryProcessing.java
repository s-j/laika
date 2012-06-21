package com.ntnu.laika.query.processing;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.DefaultPostingListIterator;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class OR_DAAT_MaxScore_QueryProcessing extends QueryProcessing{
	
	public OR_DAAT_MaxScore_QueryProcessing(DiskInvertedIndex index, Statistics stats){
		super(index, stats);
	}

	public QueryResults processQuery(Query query){
		rheap.reset();
		QueryEntry[] qEntries = query.getEntries();
		
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);

		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		double[] maxScores = new double[numTerms];
		double[] accScores = new double[numTerms];
		
		double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		double _accScore = 0.0d, _kf;
		//System.out.println("===");
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){			
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = _qE.getKeyFrequency()/maxKeyFreq;
			maxScores[i] = FastMaxScore.getMaxScore(_lE.getTermId(), _kf);
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);
		}
		/*NOTE: in LP case, we calculate scores only for (numTerms-1) last terms. Since the first one is never used anyway.
		 */
		
		for (int i = numTerms - 1; i >= 0; i--){
			_accScore += maxScores[i];
			accScores[i] = _accScore;
		}
		System.out.println("("+numTerms+") -->" + accScores[0]);
		
		int numberOfResults = 0, numReqTerms = numTerms, canddoc = -1, lastdoc, i;
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
				} else if (canddoc > iterators[i].getDocId()){
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

			if (score < reqScore) continue;
			
			if (rheap.insertIfGreaterThanLeast(canddoc, score)){
				reqScore = rheap.minScore();
				while (numReqTerms > 0 && accScores[numReqTerms-1] < reqScore) numReqTerms--;
			}
			
			numberOfResults++;
		}
		
		for (i=0; i<numTerms; i++) iterators[i].close();
		
		numberOfResults = rheap.size();
		int resdocids[] = new int[numberOfResults];
		double resscores[] = new double[numberOfResults];
		rheap.decrSortResults(resdocids, resscores);
				
		return new QueryResults(resdocids, resscores, numberOfResults, numberOfResults);
	}
}