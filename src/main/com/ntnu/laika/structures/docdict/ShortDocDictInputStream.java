package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortDocDictInputStream implements Closeable{
	private BufferWrapper buffer;
	
	private int numberOfDocuments = 0;
	
    public ShortDocDictInputStream(BufferWrapper buffer) {
        this.buffer = buffer;
    }
    
    public ShortDocDictEntry nextEntry() {
    	numberOfDocuments++;
    	int docid = buffer.getInt();
		int numTokens = buffer.getInt();
		return new ShortDocDictEntry(docid, numTokens);
    }

    @Override
    public void close() {
    	buffer.close();
    }
    
    public int numberOfDocuments(){
    	return numberOfDocuments;
    }
}
