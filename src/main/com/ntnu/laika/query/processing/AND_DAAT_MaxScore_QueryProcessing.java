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
public class AND_DAAT_MaxScore_QueryProcessing extends QueryProcessing{
	
	public AND_DAAT_MaxScore_QueryProcessing(DiskInvertedIndex index, Statistics stats){
		super(index, stats);
	}
	
	public QueryResults processQuery(Query query){
		rheap.reset();
		QueryEntry qEntries[] = query.getEntries();
		
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);

		//double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		double maxScores[] = new double[numTerms];
		double accScores[] = new double[numTerms];
		double _accScore = 0.0d, _kf;
		
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = 1.0;//_qE.getKeyFrequency()/maxKeyFreq;
			maxScores[i] = FastMaxScore.getMaxScore(_lE.getTermId(), _kf);
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);		
		}
		
		//Arrays.sort(iterators, DESCENDINGMAXSCORE);		
		for (int i = numTerms - 1; i >= 0; i--){
			_accScore += maxScores[i];
			accScores[i] = _accScore;
		}

		int numberOfResults = 0;
		int doc, _doc, i; double score, reqScore = 0.0d;
		
loop:  while (true) {
			doc = iterators[0].getDocId();
			score = iterators[0].getScore();
		
			for (i = 1; i < numTerms; i++){
				if (score + accScores[i] < reqScore) break;
			
				if (!iterators[i].skipTo(doc)) break loop;
			
				_doc = iterators[i].getDocId();
				if (_doc > doc) {
					if (iterators[0].skipTo(_doc)) continue loop;
					else break loop;
				}
			
				score += iterators[i].getScore();
			}			
		
			if (i==numTerms && rheap.insertIfGreaterThanLeast(doc, score)){
				reqScore = rheap.minScore();
			}
			
			if (!iterators[0].next()) break loop;
		}
		
		for (i=0; i<numTerms; i++) iterators[i].close();
		
		numberOfResults = rheap.size();
		int resdocids[] = new int[numberOfResults];
		double resscores[] = new double[numberOfResults];
		rheap.decrSortResults(resdocids, resscores);
		return new QueryResults(resdocids, resscores, numberOfResults, numberOfResults);
	}
}