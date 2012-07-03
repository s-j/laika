package com.ntnu.laika.query.processing;

import java.util.Arrays;

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
public class AND_DAAT_DOCIDS_ONLY_QueryProcessing extends QueryProcessing{
	
	public AND_DAAT_DOCIDS_ONLY_QueryProcessing(DiskInvertedIndex index, Statistics stats){
		super(index, stats);
		docids = new int[stats.getNumberOfDocuments()];
	}

	private int[] docids; 
	
	public QueryResults processQuery(Query query){
		//rheap.reset();
		QueryEntry qEntries[] = query.getEntries();
		
		int numTerms = qEntries.length;
		if (numTerms == 0) return new QueryResults(null, null, 0, 0);

		double maxKeyFreq = query.getMaxKeyFrequency();
		double numOfDocs = stats.getNumberOfDocuments();
		double avgDocLength = stats.getAverageDocumentLength();
		DefaultPostingListIterator iterators[] = new DefaultPostingListIterator[numTerms];
		
		double _kf;
		
		QueryEntry _qE; LexiconEntry _lE;
		for (int i=0; i<numTerms; i++){
			_qE = qEntries[i];
			_lE = _qE.getLexiconEntry();
			iterators[i] = (DefaultPostingListIterator) index.getPostingListIterator(_lE);
			_kf = ((double)_qE.getKeyFrequency())/maxKeyFreq;
			iterators[i].prepareForScoring(_kf, _lE.getN_t(), _lE.getTF(), numOfDocs, avgDocLength);
		}
				
		int numberOfResults = 0;
		int doc, _doc, i;
		
loop:  while (true) {
			doc = iterators[0].getDocId();

			for (i = 1; i < numTerms; i++){
				if (!iterators[i].skipTo(doc)) break loop;
				
				_doc = iterators[i].getDocId();
				if (_doc > doc) {
					if (iterators[0].skipTo(_doc)) continue loop;
					else break loop;
				}
			}			
		
			//score = 0.0d;
			//for (i = 0; i < numTerms; i++) score += iterators[i].getScore();				
			//rheap.insertIfGreaterThanLeast(doc, score);
			docids[numberOfResults++] = doc;
			if (!iterators[0].next()) break loop;
		}
		
		for (i=0; i<numTerms; i++) iterators[i].close();
		
		int resdocids[] = Arrays.copyOf(docids, numberOfResults);
		double resscores[] = null;
		return new QueryResults(resdocids, resscores, numberOfResults, numberOfResults);
	}
}