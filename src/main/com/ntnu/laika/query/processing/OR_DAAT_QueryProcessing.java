package com.ntnu.laika.query.processing;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.postinglist.DefaultPostingListIterator;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class OR_DAAT_QueryProcessing extends QueryProcessing{
	
	public OR_DAAT_QueryProcessing(DiskInvertedIndex index, Statistics stats){
		super(index, stats);
	}

	public QueryResults processQuery(Query query){
		rheap.reset();
		QueryEntry[] qEntries = query.getEntries();
		
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);

		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		
		double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		double _kf;
		//System.out.println("===");
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){			
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = _qE.getKeyFrequency()/maxKeyFreq;
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);
		}
		
		int numberOfResults = 0, canddoc = -1, lastdoc, i;
		double score;
		
		while (numTerms > 0) {
			//find the least pointer among the terms
			lastdoc = canddoc;
			canddoc = Integer.MAX_VALUE;
			for (i = 0; i < numTerms; i++){
				if (iterators[i].getDocId() == lastdoc && !iterators[i].next()){
					iterators[i].close();					
					numTerms--;
					for (int j=i; j<numTerms; j++) iterators[j] = iterators[j+1];
					i--;
				} else if (canddoc > iterators[i].getDocId()){
					canddoc = iterators[i].getDocId();
				}
			}
			
			score = 0.0d;				
			for (i = 0; i < numTerms; i++){
				if (iterators[i].getDocId() == canddoc) score += iterators[i].getScore();
			}

			rheap.insertIfGreaterThanLeast(canddoc, score);
		}
		
		numberOfResults = rheap.size();
		int resdocids[] = new int[numberOfResults];
		double resscores[] = new double[numberOfResults];
		rheap.decrSortResults(resdocids, resscores);
				
		return new QueryResults(resdocids, resscores, numberOfResults, numberOfResults);
	}
}