package com.ntnu.laika.structures.lexicon.global;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class GlobalLexiconOutputStream implements Closeable{
	private BufferWrapper buffer;

	private int numberOfTerms = 0;
	private long numberOfTokens = 0;
	private long numberOfPointers = 0;
	
    public GlobalLexiconOutputStream(BufferWrapper buffer){
    	this.buffer = buffer;
    }
    
    private static final byte [] zerobuffer = new byte[Constants.STRING_BYTE_LENGTH];
    
	public void nextEntry(String term, int termid, int n_t, int TF, byte[] signature) {
		byte[] termbytes = term.getBytes();
        buffer.put(termbytes, 0, termbytes.length);
        buffer.put(zerobuffer, 0, Constants.STRING_BYTE_LENGTH - termbytes.length);
        buffer.putInt(termid);
        buffer.putInt(n_t);
        buffer.putInt(TF);
        buffer.put(signature, 0, signature.length);
        //if (signature.length != GlobalLexiconEntry.MAX_NODES_SUPPORTED/8) System.out.println("too long signature!");
        numberOfTerms++; numberOfPointers += n_t; numberOfTokens += TF;
    }
	
	public void nextEntry(GlobalLexiconEntry l) {
		nextEntry(l.term, l.termid, l.n_t, l.TF, l.signature);
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
