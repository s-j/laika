package com.ntnu.laika.structures.lexicon.global;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class GlobalLexiconInputStream implements Closeable{
	private BufferWrapper buffer;
	
	private int numTermsTotal;
	private int numberOfTerms = 0;
	private long numberOfTokens = 0;
	private long numberOfPostings = 0;
	
    public GlobalLexiconInputStream(BufferWrapper buffer, int numTermsTotal) {
        this.buffer = buffer;
        this.numTermsTotal = numTermsTotal;
    }
    
    private static byte [] textbuffer = new byte[Constants.STRING_BYTE_LENGTH]; 
    
    public GlobalLexiconEntry nextEntry() {
    	if (numberOfTerms == numTermsTotal) return null;
    	buffer.get(textbuffer, 0, textbuffer.length);
		String term = new String(textbuffer).trim();
		int termid = buffer.getInt();
		int n_t = buffer.getInt();
		int TF = buffer.getInt();
		byte[] signature = new byte[GlobalLexiconEntry.MAX_NODES_SUPPORTED/8];
		buffer.get(signature, 0, signature.length);
		numberOfTerms++; numberOfPostings += n_t; numberOfTokens += TF;
		GlobalLexiconEntry ret = new GlobalLexiconEntry(term, termid, n_t, TF, signature);
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
