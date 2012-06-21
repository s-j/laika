package com.ntnu.laika.query.preprocessing;

import java.util.Arrays;
import java.util.Comparator;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryEntry;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * NOTE: The method is not thread-safe!
 * 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QueryPreprocessing{
	
	private String[] terms = new String[Constants.MAX_QUERY_LENGTH];
	private QueryEntry[] qEntries = new QueryEntry[Constants.MAX_QUERY_LENGTH];
	
	private Lexicon lexicon;
	private LexiconEntrySorter sorter;
	
	PorterStemmer stemmer;
	Stopwords stopwords;
	
	
	public QueryPreprocessing(Lexicon lexicon){
		this.lexicon = lexicon;
		sorter = new LexiconEntrySorter();
		
		stopwords = new Stopwords("/home/simonj/workstuff/Java/Laika/stopword-list.txt");
		stemmer = new PorterStemmer(); 
	}
	
	private String removeNonAlphaNumChars(String t) {
		final int t_length = t.length();
		StringBuilder buffer = new StringBuilder(t_length);
		char c;
		for (int i=0; i<t_length; i++) {
			if (Character.isLetterOrDigit(c = t.charAt(i)))
				buffer.append(c);
		}
		return buffer.toString();
	}
	
	public Query processQuery(String querystr){
		int numt=0;
		
		//to lower case
		querystr = querystr.toLowerCase();
		String tmp;
		for (String token : querystr.split(" ")){
			//remove nonalphanumerics
			//nonalpharemove -> stopwordremove -> stemming -> queryproc
			tmp=stemmer.processTerm(
							stopwords.processTerm(
								removeNonAlphaNumChars(token)));
			if (tmp!=null) terms[numt++]=tmp;
		}
		
		if (numt == 0) return null;
		
		Arrays.sort(terms,0,numt);
		
		LexiconEntry _lEntry = null;
			
		int i=0;
		
		while (i <= numt && (_lEntry = lexicon.lookup(terms[i]))==null){
			i++;
		}
		
		if (i > numt) return null;
		
		int _numt = 0;
		qEntries[_numt++] = new QueryEntry(_lEntry, 1);
		String _lastTerm = _lEntry.getTerm();
		i++;
		
		for (; i<numt; i++){
			if (_lastTerm.equals(terms[i])){
				qEntries[_numt].incrementKeyFrequency();
			} else {
				if ( (_lEntry = lexicon.lookup(terms[i])) != null ){
					qEntries[_numt++] = new QueryEntry(_lEntry, 1);
				}
			}
		}

		QueryEntry _qEntries[] = new QueryEntry[_numt];
		for (i=0; i<_numt; i++) {
			_qEntries[i] = qEntries[i];
		}
		
		Arrays.sort(_qEntries, sorter);
		return new Query(_qEntries);
	}

	private class LexiconEntrySorter implements Comparator<QueryEntry>{
		@Override
		public int compare(QueryEntry o1, QueryEntry o2) {
			if (o1.getLexiconEntry().getN_t() < o2.getLexiconEntry().getN_t()) return -1;
			else if (o1.getLexiconEntry().getN_t() > o2.getLexiconEntry().getN_t()) return 1;
			else return 0;
		}
	}
}
 