package com.ntnu.laika.query.processing;

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
public class AND_DAAT_LP_MaxScore2_QueryProcessing extends QueryProcessing{
	
	public AND_DAAT_LP_MaxScore2_QueryProcessing(DiskInvertedIndex index, Statistics stats){		
		super(index, stats);
		System.out.println("AND_MS_LP2");
	}

	public static boolean doLP = true;
	
	public QueryResults processQuery(Query query){
		rheap.reset();
		QueryEntry[] qEntries = query.getEntries();
		
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);

		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		double[] maxScores = new double[numTerms];
		double[] accScores = new double[numTerms];
		int[] ids = new int[numTerms];
		
		//double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		double _accScore = 0.0d, _kf;
		
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){			
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = 1.0;//_qE.getKeyFrequency()/maxKeyFreq;
			maxScores[i] = FastMaxScore.getMaxScore(_lE.getTermId(), _kf);
			ids[i] = _lE.getTermId();
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);
		}
		
		accScoreTime = System.nanoTime();
		if (doLP && numTerms > 2 && numTerms <= LPSolver.MAX_QUERY_TERM_COUNT) {
			AccScorePredictor.fillAccScoresAND(accScores, maxScores, ids, numTerms); 
		} else {
			for (int i = numTerms - 1; i >= 0; i--){
				_accScore += maxScores[i];
				accScores[i] = _accScore;
			}
		}
		procLoopTime = System.nanoTime();
		accScoreTime = procLoopTime - accScoreTime;
		
		int numberOfResults = 0;
		int doc, _doc, i; double score, reqScore = 0.0d;
	   
loop:  while (true) {
			doc = iterators[0].getDocId();
			for (i = 1; i < numTerms ; i++){
				if (iterators[i].skipTo(doc)){
					_doc = iterators[i].getDocId();
					if (_doc > doc) {
						if (iterators[0].skipTo(_doc)) continue loop;
						else break loop;
					}
				} else break loop;
			}
			
			score = iterators[0].getScore();
			for (i = 1; i < numTerms && score + accScores[i] >= reqScore; i++){
				score += iterators[i].getScore();
			}
		
			if (i==numTerms && rheap.insertIfGreaterThanLeast(doc, score)){
				//SimpleMultiStats.addDescription(3, 1);
				reqScore = rheap.minScore();
			}
			
			if (!iterators[0].next()) break loop;
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