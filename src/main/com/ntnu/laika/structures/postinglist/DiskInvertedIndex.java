package com.ntnu.laika.structures.postinglist;

import java.io.IOException;

import com.ntnu.laika.buffering.BufferPool;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.structures.FileWrapper;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.ShortLexiconEntry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DiskInvertedIndex implements InvertedIndex {
	private BufferPool bufferpool;
	private int fileNumber;
	
	public DiskInvertedIndex(BufferPool bufferpool, int fileNumber){
		this.bufferpool = bufferpool;
		this.fileNumber = fileNumber;
	}
	
	public PostingListIterator getPostingListIterator(LexiconEntry lEntry){
		try {
			long startOffset = lEntry.getStartOffset();
			//new buffer wrapper, also fetch the first block of the posting list at once!
			BufferWrapper buffer = new FileWrapper(bufferpool, fileNumber, startOffset);
			//set the address to zero as it is relative to the buffer's start address.
			return new DefaultPostingListIterator(buffer, lEntry.getN_t(), 0);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}	
	
	public PostingListIterator getPostingListIterator(ShortLexiconEntry lEntry){
		//System.out.println("init!");
		try {
			long startOffset = lEntry.getStartOffset();
			//new buffer wrapper, also fetch the first block of the posting list at once!
			
			BufferWrapper buffer = new FileWrapper(bufferpool, fileNumber, startOffset);
			//System.out.println("??");
			//set the address to zero as it is relative to the buffer's start address.
			PostingListIterator p= new DefaultPostingListIterator(buffer, lEntry.getN_t(), 0);
			//System.out.println("ok");
			return p;
		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
		return null;
	}	
}
