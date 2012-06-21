package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortDocDictOutputStream implements Closeable{
	private BufferWrapper buffer;

	private int numberOfDocuments = 0;
	
    public ShortDocDictOutputStream(BufferWrapper buffer) {
    	this.buffer = buffer;
    }
    
    public void nextEntry(int docid, int numTokens) {
		buffer.putInt(docid);
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
