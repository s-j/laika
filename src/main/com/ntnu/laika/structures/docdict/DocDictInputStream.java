package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DocDictInputStream implements Closeable{
	private BufferWrapper buffer;
	
	private int numberOfDocuments = 0;
	
    public DocDictInputStream(BufferWrapper buffer) {
        this.buffer = buffer;
    }
    
    private static byte [] textbuffer = new byte[Constants.DOCNO_BYTE_LENGTH]; 
    
    public DocDictEntry nextEntry() {
    	numberOfDocuments++;
    	int docid = buffer.getInt();
    	buffer.get(textbuffer, 0, textbuffer.length);
		String docno = new String(textbuffer).trim();
		
		int numTokens = buffer.getInt();
		return new DocDictEntry(docid, docno, numTokens);
    }

    @Override
    public void close() {
    	buffer.close();
    }
    
    public int numberOfDocuments(){
    	return numberOfDocuments;
    }
}
