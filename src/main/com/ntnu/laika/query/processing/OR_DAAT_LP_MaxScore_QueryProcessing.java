package com.ntnu.laika.query.processing;

import java.util.Arrays;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.DefaultPostingListIterator;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
import com.ntnu.lpsolver.AccScorePredictor;
import com.ntnu.lpsolver.LPSolver;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class OR_DAAT_LP_MaxScore_QueryProcessing extends QueryProcessing{
	
	public OR_DAAT_LP_MaxScore_QueryProcessing(DiskInvertedIndex index, Statistics stats){
		super(index, stats);
	}

	public static boolean doLP = true;
	
	public QueryResults processQuery(Query query){
		rheap.reset();
		QueryEntry[] qEntries = query.getEntries();
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);
		boolean lp = doLP && numTerms > 2 && numTerms <= LPSolver.MAX_QUERY_TERM_COUNT;
		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		double[] maxScores = new double[numTerms];
		double[] accScores = new double[numTerms];
		int[] ids = new int[numTerms];
		//double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		double _accScore = 0.0d, _kf;
		//System.out.println("===");
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){			
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = 1.0;//_qE.getKeyFrequency()/maxKeyFreq;
			maxScores[i] = FastMaxScore.getMaxScore(_lE.getTermId(), _kf);
			ids[i] = _lE.getTermId();
			//System.out.println(_lE.getN_t()+" "+maxScores[i]);
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);
		}
		/*NOTE: in LP case, we calculate scores only for (numTerms-1) last terms. Since the first one is never used anyway.
		 */
		double[] lpHelperTable = null;
		int _ids[] = null;
		_ids = Arrays.copyOf(ids, ids.length);
		
		accScoreTime = System.nanoTime();
		if (lp) {
			lpHelperTable = AccScorePredictor.getAccScoresORTable(ids, maxScores);
			for (int i=0; i<numTerms; i++)
				accScores[i] = lpHelperTable[AccScorePredictor.getAccScoresORIndex(ids, _ids, i, numTerms)];
			
			//for (double ms : maxScores) _accScore += ms;
			//if (accScores[0] > _accScore){
				//for (QueryEntry qE : qEntries) System.out.print(qE.getLexiconEntry().getTerm()+" ");	
				//System.out.println(":" + accScores[0]+" "+_accScore);
			//}
		} else {
			for (int i = numTerms - 1; i >= 0; i--){
				_accScore += maxScores[i];
				accScores[i] = _accScore;
			}
		}
		
		procLoopTime = System.nanoTime();
		accScoreTime = procLoopTime - accScoreTime;
		
		int numberOfResults = 0, numReqTerms = numTerms, lastdoc, canddoc = -1, i;
		double score, reqScore = 0.0d, _decscore;
		
oloop:	while (numReqTerms > 0) {
			//find the least pointer among OR-terms
			lastdoc = canddoc;
			canddoc = Integer.MAX_VALUE;
			for (i = 0; i < numReqTerms; i++){
				if (iterators[i].getDocId() == lastdoc && !iterators[i].next()){
					//close iterator, update scores above, move those from below up
					iterators[i].close();
					numTerms--;
					
					//move those on the right side
					for (int j=i; j<numTerms; j++){
						iterators[j] = iterators[j+1];
						maxScores[j] = maxScores[j+1];
						accScores[j] = accScores[j+1];
						_ids[j] = _ids[j+1];
					}

					//update those on the left side
					if (lp){
						for (int j=0; j<i; j++)
							accScores[j] = lpHelperTable[AccScorePredictor.getAccScoresORIndex(
									ids, _ids, j, numTerms)];
					} else {
						_decscore = maxScores[i];
						for (int j=0; j<i; j++) accScores[j] -= _decscore;
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
					iterators[i].close();
					numTerms--;
					//move those on the right side
					for (int j=i; j<numTerms; j++) {
						maxScores[j] = maxScores[j+1];
						accScores[j] = accScores[j+1];
							 _ids[j] = _ids[j+1];
						iterators[j] = iterators[j+1];
					}
					
					//update those on the left side
					if (lp){
						for (int j=0; j<i; j++)
							accScores[j] = lpHelperTable[AccScorePredictor.getAccScoresORIndex(
									ids, _ids, j, numTerms)];
					} else {
						_decscore = maxScores[i];
						for (int j=0; j < i; j++){
							accScores[j] -= _decscore;
							_ids[j] = _ids[j+1];
						}				
					}
					i--;	
				} else if (iterators[i].getDocId() == canddoc) {
					score += iterators[i].getScore();
				}
			}

			if (score < reqScore) continue;
			
			if (rheap.insertIfGreaterThanLeast(canddoc, score)){
				//SimpleMultiStats.addDescription(3, 1);
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
		
		procLoopTime = System.nanoTime() - procLoopTime;
		numQueries++;
		//System.out.println(accScoreTime + " " + procLoopTime + " " + numQueries);
		return new QueryResults(resdocids, resscores, numberOfResults, numberOfResults);
	}
}