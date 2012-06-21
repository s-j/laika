package com.ntnu.laika.structures.postinglist;

import com.ntnu.laika.utils.Closeable;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface PostingListIterator extends Closeable{
	
	/**
	 * Skips to the specified docid
	 * @param docid
	 * @return false if EOF or if the specified docID is less than the current docID
	 */
	public boolean skipTo(int docid);
	
	/**
	 * Fetches the next entry
	 * @return false if EOF
	 */
	public boolean next();
	
	/**
	 * Returns current docID
	 * @return
	 */
	public int getDocId();
	
	/**
	 * returns current frequency
	 * @return
	 */
	public int getFrequency();
	
	/**
	 * returns current frequency
	 * @return
	 */
	public double getScore();
}
