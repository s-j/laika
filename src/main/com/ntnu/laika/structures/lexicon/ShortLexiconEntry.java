package com.ntnu.laika.structures.lexicon;

import com.ntnu.laika.Constants;

/**
 * Similar to the lexicon entry, but does not contain the textual representation of the term itself.
 * Contains: termid, nt, tf and the end offset.
 * 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ShortLexiconEntry{
	public static final int ENTRY_SIZE = Constants.INT_SIZE * 3 + Constants.LONG_SIZE; //id,nt,tf,endptr
	
	protected int termid;
	protected int n_t;
	protected int TF;
	protected long startOffset;
	protected long   endOffset;
	
	public ShortLexiconEntry(int termid, int n_t, int TF, long startOffset, long endOffset){
		this.termid = termid;
		this.n_t = n_t;
		this.TF = TF;
		
		this.startOffset = startOffset;
		this.endOffset = endOffset;
	}

	public final int getTermId(){
		return termid;
	}
	
	public final void setTermId(int termid){
		this.termid = termid;
	}

	public final int getN_t() {
		return n_t;
	}

	public final void setN_t(int nT) {
		n_t = nT;
	}

	public final int getTF() {
		return TF;
	}

	public final void setTF(int tF) {
		TF = tF;
	}

	public final long getStartOffset() {
		return startOffset;
	}

	public final void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public final long getEndOffset() {
		return endOffset;
	}

	public final void setEndOffset(long endOffset) {
		this.endOffset = endOffset;
	}
	
	public final String toString(){
		return "(" + termid + ") " + n_t + " " + TF + " (" + startOffset +" - " + endOffset +")";
	}
}
