package com.ntnu.laika.structures.lexicon;

import java.io.IOException;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Lexicon implements Closeable{
	protected final int totEntries;
	protected BufferWrapper buffer;
		
    public Lexicon(BufferWrapper buffer, int entries) {
        this.buffer = buffer;
        totEntries = entries;
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
			_term = getLexiconString(i);
			if ((compareStrings = term.compareTo(_term)) < 0) high = i - 1;
			else if (compareStrings > 0) low = i + 1;
			else return getLexiconEntry(i);
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
			_termId = getLexiconId(i);
			if ( termId < _termId) high = i - 1;
			else if ( termId > _termId) low = i + 1;
			else return getLexiconEntry(i);
		}
		
		return null;
    }
    
    private static byte [] textbuffer = new byte[Constants.STRING_BYTE_LENGTH]; 
    
    /**
     * Get the term string representation of a lexicon entry.
     * 
     * @param entryNumber the number of the lexicon entry
     * @return term
     * @throws IOException
     * @throws InterruptedException
     */
	private String getLexiconString(long entryNumber){
    	buffer.position(entryNumber * LexiconEntry.ENTRY_SIZE);
		buffer.get(textbuffer, 0, textbuffer.length);
		return new String(textbuffer).trim();
    }
    
    /**
     * Get the term ID of a lexicon entry.
     * 
     * @param entryNumber the number of the lexicon entry
     * @return term ID
     * @throws IOException
     * @throws InterruptedException
     */
    private int getLexiconId(long entryNumber){ 
    	buffer.position(entryNumber * LexiconEntry.ENTRY_SIZE + Constants.STRING_BYTE_LENGTH);
		return buffer.getInt();
    }
    
    /**
     * Get a lexicon entry.
     * 
     * @param entryNumber the number of the lexicon entry
     * @return the lexicon entry itself
     * @throws IOException
     * @throws InterruptedException
     */
    private LexiconEntry getLexiconEntry(long entryNumber){
    	long startOffset = 0;
    	if (entryNumber > 0) {
    		buffer.position(entryNumber * LexiconEntry.ENTRY_SIZE - Constants.LONG_SIZE);
    		startOffset = buffer.getLong();
    	} else {
    		buffer.position(entryNumber * LexiconEntry.ENTRY_SIZE);
    	}
    	
		buffer.get(textbuffer, 0, textbuffer.length);
		String term = new String(textbuffer).trim();
		int termid = buffer.getInt();
		int n_t = buffer.getInt();
		int TF = buffer.getInt();
		long endOffset = buffer.getLong();
		 
		return new LexiconEntry(term, termid, n_t, TF, startOffset,  endOffset);	
    }
    
    public long getEndOffset(){
    	return getLexiconEntry(totEntries-1).endOffset;
    }
    

    /**
     * Closes the lexicon, unpins the current buffer, etc.
     */
    @Override
    public void close() {
    	buffer.close();
    }
}