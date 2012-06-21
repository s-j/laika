package com.ntnu.laika.structures.postinglist;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.LexiconInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class PostingListInputStream implements PostingListIterator{
	public BufferWrapper buffer;
	protected LexiconInputStream lexiconInput;
	
	public PostingListInputStream(BufferWrapper buffer, LexiconInputStream lexiconInput){
		this.buffer = buffer;
		this.lexiconInput = lexiconInput;
	}
	
	private LexiconEntry lEntry = null;
	private PostingListIterator iter = null;

	public LexiconEntry nextEntry(){
		lEntry = lexiconInput.nextEntry();
		//System.out.println(lEntry.getTermId()+" "+lEntry.getTerm()+" "+lEntry.getN_t()+" "+lEntry.getTF());
		if (lEntry != null)
			iter = new DefaultPostingListIterator(buffer, lEntry.getN_t(), lEntry.getStartOffset());
		return lEntry;
	}

	@Override
	public int getDocId() {
		if (iter != null) return iter.getDocId();
		else return -1;
	}

	@Override
	public int getFrequency() {
		if (iter != null) return iter.getFrequency();
		else return -1;
	}

	@Override
	public boolean next() {
		if (iter != null) return iter.next();
		else return false;
	}

	@Override
	public boolean skipTo(int docid) {
		if (iter != null) return iter.skipTo(docid);
		else return false;
	}	
	
	@Override
	public void close() {
		if (iter!=null) iter.close();
		else buffer.close();
		lexiconInput.close();
	}

	@Override
	public double getScore() {
		throw new NotImplementedException();
	}
}
