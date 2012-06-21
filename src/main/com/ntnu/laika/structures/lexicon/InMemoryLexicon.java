package com.ntnu.laika.structures.lexicon;

import java.io.IOException;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class InMemoryLexicon extends Lexicon implements Closeable{
	private LexiconEntry[] entries;
	
    public InMemoryLexicon(BufferWrapper buffer, int totEntries) {
    	super(buffer, totEntries);
		entries = new LexiconEntry[totEntries];
    	LexiconInputStream input = new LexiconInputStream(buffer, totEntries);
    	for (int i = 0; i < totEntries; i++) entries[i] = input.nextEntry();
    	input.close();
      	buffer.close();
      	buffer = null;
    	//System.out.println("lexicon is loaded");
    }
    
    /**
     * Look up LexiconEntry with a specified term
     * @param term
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public synchronized LexiconEntry lookup(String term) {
    	int low = 0;
		int high = totEntries - 1;
		int i;
		int compareStrings;
		String _term;
		
		while (high >= low) {
			i = (high + low) >> 1;				
			_term = entries[i].term;
			if ((compareStrings = term.compareTo(_term)) < 0) high = i - 1;
			else if (compareStrings > 0) low = i + 1;
			else return entries[i];
		}
		
		return null;
    }
    
    /**
     * Look up LexiconEntry with a specified term ID
     * @param termId
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public synchronized LexiconEntry lookup(int termId){
      	int low = 0;
      	int high = totEntries - 1;
      	int i;
		int _termId;
		
		while (high >= low) {
			i = (high + low) >> 1;
			_termId = entries[i].termid;
			if ( termId < _termId) high = i - 1;
			else if ( termId > _termId) low = i + 1;
			else return entries[i];
		}
		
		return null;
    }
    
  
  @Override
  public void close(){}
}