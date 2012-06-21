package com.ntnu.laika.structures.docdict;

import com.ntnu.laika.Constants;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DocDictEntry{
	public static final int ENTRY_SIZE = Constants.DOCNO_BYTE_LENGTH + Constants.INT_SIZE * 2; //id, docno, numtokens
	
	protected int docid;
	protected String docno;
	protected int numTokens;
	
	public DocDictEntry(int docid, String docno, int numTokens){
		this.docid = docid;
		this.docno = docno;
		this.numTokens = numTokens;
	}

	public final int getDocid() {
		return docid;
	}

	public final void setDocid(int docid) {
		this.docid = docid;
	}

	public final String getDocno() {
		return docno;
	}

	public final void setDocno(String docno) {
		this.docno = docno;
	}

	public final int getNumberOfTokens() {
		return numTokens;
	}

	public final void setNumberOfTokens(int numTokens) {
		this.numTokens = numTokens;
	}
}
