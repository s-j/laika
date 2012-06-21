package com.ntnu.laika.query;

import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QueryEntry{
	protected int keyFrequency;
	protected LexiconEntry lexiconEntry;
	
	
	public QueryEntry(LexiconEntry lEntry, int kFrequency){
		lexiconEntry = lEntry;
		keyFrequency = kFrequency;
	}
	
	public final int getKeyFrequency(){
		return keyFrequency;
	}
	
	public final LexiconEntry getLexiconEntry(){
		return lexiconEntry;
	}
	
	public final void incrementKeyFrequency(){
		keyFrequency++;
	}
	
	public final String toString(){
		return lexiconEntry + ":" + keyFrequency;
	}
}
