package com.ntnu.laika.structures.lexicon;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortLexiconInputStream implements Closeable{
	private BufferWrapper buffer;
	
	private int numTermsTotal;
	private int numberOfTerms = 0;
	private long numberOfTokens = 0;
	private long numberOfPostings = 0;
	private long lastEndOffset = 0;
	
    public ShortLexiconInputStream(BufferWrapper buffer, int numTermsTotal) {
        this.buffer = buffer;
        this.numTermsTotal = numTermsTotal;
    }
    
    public ShortLexiconEntry nextEntry() {
    	if (numberOfTerms == numTermsTotal) return null;

    	int termid = buffer.getInt();
		int n_t = buffer.getInt();
		int TF = buffer.getInt();
		long endOffset = buffer.getLong();
		
		numberOfTerms++; numberOfPostings += n_t; numberOfTokens += TF;
		ShortLexiconEntry ret = new ShortLexiconEntry(termid, n_t, TF, lastEndOffset,  endOffset);
		lastEndOffset = endOffset;
		return ret;
    }

    @Override
    public void close() {
    	buffer.close();
    }
    
    public int numberOfTerms(){
    	return numberOfTerms;
    }

    public long numberOfTokens(){
    	return numberOfTokens;
    }
    
    public long numberOfPostings(){
    	return numberOfPostings;
    }
}
