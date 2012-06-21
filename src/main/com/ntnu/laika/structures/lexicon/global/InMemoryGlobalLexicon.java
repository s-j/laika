package com.ntnu.laika.structures.lexicon.global;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class InMemoryGlobalLexicon extends GlobalLexicon implements Closeable{
	private GlobalLexiconEntry[] earray;
	private int totEntries;
	
	public InMemoryGlobalLexicon(BufferWrapper buffer, int entries) {
		super(null, 0);
		totEntries = entries;
		earray = new GlobalLexiconEntry[entries];
    	GlobalLexiconInputStream input = new GlobalLexiconInputStream(buffer, entries);
    	for (int i = 0; i < entries; i++) earray[i] = input.nextEntry();
    	input.close();
    	System.out.println("--lexicon is loaded");
    }
    
    public GlobalLexiconEntry lookup(String term) {
    	int low = 0, high = totEntries - 1,  i, compareStrings;
			
		while (high >= low) {
			i = (high + low) >> 1;
			if ((compareStrings = term.compareTo(earray[i].term)) < 0) high = i - 1;
			else if (compareStrings > 0) low = i + 1;
			else return earray[i];
		}
		
		return null;
    }
    
    public GlobalLexiconEntry lookup(int termId){
    	int low = 0;
      	int high = totEntries - 1;
      	int i;
		int _termId;
		
		while (high >= low) {
			i = (high + low) >> 1;
			_termId = earray[i].termid;
			if ( termId < _termId) high = i - 1;
			else if ( termId > _termId) low = i + 1;
			else return earray[i];
		}
		
		return null;
	}
    
    @Override
    public void close() {}
}