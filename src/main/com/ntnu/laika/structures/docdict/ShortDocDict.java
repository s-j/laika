package com.ntnu.laika.structures.docdict;

import java.io.IOException;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortDocDict implements Closeable{
	private final int totEntries;
	private BufferWrapper buffer;
	
    public ShortDocDict(BufferWrapper buffer, int entries) {
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
    public ShortDocDictEntry lookup(int docId) {
      	int low = 0;
      	int high = totEntries - 1;
      	int i;
		int _docId;
		
		synchronized (buffer) {	
			while (high >= low) {
				i = (high + low) >> 1;
				_docId = getDocId(i);
				if ( docId < _docId) high = i - 1;
				else if ( docId > _docId) low = i + 1;
				else return getDocDictEntry(i);
			}
		}
		
		return null;
    }
    
    /**
     * Get the doc ID of the specified entry.
     * 
     * @param entryNumber the number of the docdict entry
     * @return term ID
     * @throws IOException
     * @throws InterruptedException
     */
    private int getDocId(long entryNumber) { 
    	buffer.position(entryNumber * ShortDocDictEntry.ENTRY_SIZE);
		return buffer.getInt();
    }
    
    /**
     * Get a docdict entry.
     * 
     * @param entryNumber the number of the docdict entry
     * @return the docdict entry itself
     * @throws IOException
     * @throws InterruptedException
     */
    private ShortDocDictEntry getDocDictEntry(long entryNumber) {
    	buffer.position(entryNumber * ShortDocDictEntry.ENTRY_SIZE);
		int docid = buffer.getInt();    	
		int numTokens = buffer.getInt();
		return new ShortDocDictEntry(docid, numTokens);	
    }

    /**
     * Closes the lexicon, unpins the current buffer, etc.
     */
    @Override
    public void close() {
    	buffer.close();
    }
}