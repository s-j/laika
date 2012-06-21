package com.ntnu.laika.structures.lexicon.global;

import com.ntnu.laika.Constants;

/**
 * Represents a single global lexicon entry. <termid, term, nt, tf, signature>.
 * 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class GlobalLexiconEntry{
	public static final int MAX_NODES_SUPPORTED = 8;
	public static final int ENTRY_SIZE = Constants.STRING_BYTE_LENGTH + Constants.INT_SIZE * 3 + MAX_NODES_SUPPORTED/8;
	
	protected int termid;
	protected String term;
	protected int n_t;
	protected int TF;
	protected byte[] signature;
	
	public GlobalLexiconEntry(String term, int termid, int n_t, int TF, byte[] signature){
		this.term = term;
		this.termid = termid;
		this.n_t = n_t;
		this.TF = TF;
		this.signature = signature;
	}

	public final int getTermId(){
		return termid;
	}
	
	public final void setTermId(int termid){
		this.termid = termid;
	}
	
	public final String getTerm() {
		return term;
	}

	public final void setTerm(String term) {
		this.term = term;
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

	public final byte[] getSignature() {
		return signature;
	}
	
	public String signatureToString(){
		char[] ch = new char[signature.length*8];
		for (int i=0; i<signature.length; i++)
			for (int j=0; j<8;j++)
				ch[i*8+j] = ((signature[i] & 1<<j) != 0) ? '1' : '0';
		return new String(ch);
	}

	public final void setSignature(byte[] signature) {
		this.signature = signature;
	}
	
	public final String toString(){
		return term + " (" + termid + ") " + n_t + " " + TF + " (" + signatureToString() +")";
	}
}
