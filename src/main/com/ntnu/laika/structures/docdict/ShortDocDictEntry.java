package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.Constants;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortDocDictEntry{
	public static final int ENTRY_SIZE = Constants.INT_SIZE * 2; //id, numtokens
	
	protected int docid;
	protected int numTokens;
	
	public ShortDocDictEntry(int docid, int numTokens){
		this.docid = docid;
		this.numTokens = numTokens;
	}

	public final int getDocid() {
		return docid;
	}

	public final void setDocid(int docid) {
		this.docid = docid;
	}

	public final int getNumberOfTokens() {
		return numTokens;
	}

	public final void setNumberOfTokens(int numTokens) {
		this.numTokens = numTokens;
	}
}
