package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DocDictOutputStream implements Closeable{
	private BufferWrapper buffer;

	private int numberOfDocuments = 0;
	
    public DocDictOutputStream(BufferWrapper buffer) {
    	this.buffer = buffer;
    }
    
    private static final byte [] zerobuffer = new byte[Constants.DOCNO_BYTE_LENGTH];
    
	public void nextEntry(int docid, String docno, int numTokens) {
		buffer.putInt(docid);
		byte[] termbytes = docno.getBytes();
		buffer.put(termbytes, 0, termbytes.length);
        buffer.put(zerobuffer, 0, Constants.DOCNO_BYTE_LENGTH - termbytes.length);
        buffer.putInt(numTokens);
        numberOfDocuments++;
    }
	
    @Override
    public void close() {
    	buffer.close();
    }
    
    public long numberOfDocuments(){
    	return numberOfDocuments;
    }
}
