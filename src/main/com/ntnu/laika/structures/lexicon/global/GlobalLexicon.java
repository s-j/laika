package com.ntnu.laika.structures.lexicon.global;

import java.io.IOException;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class GlobalLexicon implements Closeable{
	private final int totEntries;
	private BufferWrapper buffer;
	
    public GlobalLexicon(BufferWrapper buffer, int entries) {
        this.buffer = buffer;
        totEntries = entries;
    }
    
    /**
     * Look up GlobalLexiconEntry with a specified term
     * @param term
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public synchronized GlobalLexiconEntry lookup(String term) {
    	int low = 0;
		int high = totEntries - 1;
		int i;
		int compareStrings;
		String _term;
			
		while (high >= low) {
			i = (high + low) >> 1;				
			_term = getLexiconString(i);
			if ((compareStrings = term.compareTo(_term))< 0) high = i - 1;
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
    public synchronized GlobalLexiconEntry lookup(int termId){
      	int low  = 0;
      	int high = totEntries - 1;
      	int i;
		int _termId;
		
		while (high >= low) {
			i = (high + low)>>1;
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
    	buffer.position(entryNumber * GlobalLexiconEntry.ENTRY_SIZE);
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
    	buffer.position(entryNumber * GlobalLexiconEntry.ENTRY_SIZE + Constants.STRING_BYTE_LENGTH);
		return buffer.getInt();
    }
    
    /**
     * Get a global lexicon entry.
     * 
     * @param entryNumber the number of the lexicon entry
     * @return the lexicon entry itself
     * @throws IOException
     * @throws InterruptedException
     */
    private GlobalLexiconEntry getLexiconEntry(long entryNumber){
   		buffer.position(entryNumber * GlobalLexiconEntry.ENTRY_SIZE);
   		
		buffer.get(textbuffer, 0, textbuffer.length);
		String term = new String(textbuffer).trim();
		int termid = buffer.getInt();
		int n_t = buffer.getInt();
		int TF = buffer.getInt();
		byte[] signature = new byte[GlobalLexiconEntry.MAX_NODES_SUPPORTED>>3];
		buffer.get(signature, 0, signature.length);
		
		return new GlobalLexiconEntry(term, termid, n_t, TF, signature);	
    }

    /**
     * Closes the global lexicon, unpins the current buffer, etc.
     */
    @Override
    public void close() {
    	buffer.close();
    }
}