package com.ntnu.laika.structures.lexicon;

/**
 * Similar to Lexicon, but does not contain information the term string itself.
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FastShortLexicon {
	private static int totEntries;
	private static ShortLexiconEntry[] entries;
	
	public static void setEntries(int numTerms, ShortLexiconInputStream inputStream){
		totEntries = numTerms;
		entries = new ShortLexiconEntry[numTerms];
		for (int i=0; i<numTerms; i++) entries[i] = inputStream.nextEntry();
		inputStream.close();
	}
	    
    /**
     * Look up LexiconEntry with a specified term ID
     * @param termId
     * @return
     */
    public static ShortLexiconEntry getLexiconEntry(int termId){
    	int low = 0;
      	int high = totEntries - 1;
      	int i;
		int _termId;
		
		while (high >= low) {
			i = (high + low) >> 1;
			_termId = entries[i].termid;
			if ( termId < _termId) high = i - 1;
			else if ( termId > _termId)	low = i + 1;
			else return entries[i];
		}
		
		return null;
    }    
}