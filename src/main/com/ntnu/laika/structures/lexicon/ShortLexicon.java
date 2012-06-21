package com.ntnu.laika.structures.lexicon;

import java.io.IOException;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * Similar to Lexicon, but does not contain information the term string itself.
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortLexicon implements Closeable{
	private final int totEntries;
	private BufferWrapper buffer;
	
    public ShortLexicon(BufferWrapper buffer, int entries) {
        this.buffer = buffer;
        totEntries = entries;
    }
    
    /**
     * Look up LexiconEntry with a specified term ID
     * @param termId
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public synchronized ShortLexiconEntry lookup(int termId){
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
    
    /**
     * Get the term ID of a lexicon entry.
     * 
     * @param entryNumber the number of the lexicon entry
     * @return term ID
     * @throws IOException
     * @throws InterruptedException
     */
    private int getLexiconId(long entryNumber){ 
    	buffer.position(entryNumber * ShortLexiconEntry.ENTRY_SIZE );
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
    private ShortLexiconEntry getLexiconEntry(long entryNumber){
    	long startOffset = 0;
    	if (entryNumber > 0) {
    		buffer.position(entryNumber * ShortLexiconEntry.ENTRY_SIZE - Constants.LONG_SIZE);
    		startOffset = buffer.getLong();
    	} else {
    		buffer.position(entryNumber * ShortLexiconEntry.ENTRY_SIZE);
    	}
    	
		int termid = buffer.getInt();
		int n_t = buffer.getInt();
		int TF = buffer.getInt();
		long endOffset = buffer.getLong();
		 
		return new ShortLexiconEntry(termid, n_t, TF, startOffset,  endOffset);
    }

    /**
     * Closes the lexicon, unpins the current buffer, etc.
     */
    @Override
    public void close() {
    	buffer.close();
    }
}