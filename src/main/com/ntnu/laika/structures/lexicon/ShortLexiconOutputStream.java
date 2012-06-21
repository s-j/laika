package com.ntnu.laika.structures.lexicon;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortLexiconOutputStream implements Closeable{
	private BufferWrapper buffer;

	private int numberOfTerms = 0;
	private long numberOfTokens = 0;
	private long numberOfPointers = 0;
	
    public ShortLexiconOutputStream(BufferWrapper buffer){
    	this.buffer = buffer;
    }
    
	public void nextEntry(int termid, int n_t, int TF, long endOffset) {
	    buffer.putInt(termid);
        buffer.putInt(n_t);
        buffer.putInt(TF);
        buffer.putLong(endOffset);
        
        numberOfTerms++; numberOfPointers += n_t; numberOfTokens += TF;
    }
	
	public void nextEntry(ShortLexiconEntry l) {
		nextEntry(l.termid, l.n_t, l.TF, l.endOffset);
    }
	
    @Override
    public void close() {
    	buffer.close();
    }
    
    public int getNumberOfTerms(){
    	return numberOfTerms;
    }

    public long getNumberOfTokens(){
    	return numberOfTokens;
    }
    
    public long getNumberOfPointers(){
    	return numberOfPointers;
    }
}
