package com.ntnu.laika.query.preprocessing;

import java.util.Arrays;
import java.util.Comparator;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * NOTE: The method is not thread-safe!
 * 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class SimpleQueryPreprocessing{
	private Lexicon lexicon;
	private QueryEntry[] qEntries = new QueryEntry[Constants.MAX_QUERY_LENGTH];
	
	public SimpleQueryPreprocessing(Lexicon lexicon){
		this.lexicon = lexicon;
	}
	
	public Query processQuery(String querystr){
		LexiconEntry _lEntry = null;

		int numTerms=0;
		for (String token : querystr.split(" ")){
			if (_lEntry == null || !_lEntry.getTerm().equals(token)){
				_lEntry = lexicon.lookup(token);
				if (_lEntry != null){
					if (FastMaxScore.getMaxScore(_lEntry.getTermId())>0.0d)
						qEntries[numTerms++] = new QueryEntry(_lEntry, 1);
					else	//0 or negative maxScore, prevent from incrementing KF on next term
						_lEntry = null;
				}
					
			} else {
				qEntries[numTerms-1].incrementKeyFrequency(); 
			}
		}
		
		if (numTerms == 0) return null;
		
		Arrays.sort(qEntries, 0, numTerms, LISTLENGTHORDER);
		QueryEntry[] _qEntries = new QueryEntry[numTerms];
		for (int i=0; i<numTerms; i++) _qEntries[i] = qEntries[i];
		
		return new Query(_qEntries);
	}

	private Comparator<QueryEntry> LISTLENGTHORDER = new Comparator<QueryEntry>(){
		@Override
		public int compare(QueryEntry o1, QueryEntry o2) {
			if (o1.getLexiconEntry().getN_t() < o2.getLexiconEntry().getN_t()) return -1;
			else if (o1.getLexiconEntry().getN_t() > o2.getLexiconEntry().getN_t()) return 1;
			else return 0;
		}
	};
}
