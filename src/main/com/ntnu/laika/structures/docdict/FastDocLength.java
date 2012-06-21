package com.ntnu.laika.structures.docdict;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FastDocLength {
	private static int[] lengths;
	
	public static void setLengths(int totEntries, DocDictInputStream inputStream){
		lengths = new int[totEntries];
		for (int i=0; i<totEntries; i++){
			DocDictEntry ddEntry = inputStream.nextEntry();
			lengths[ddEntry.docid] = ddEntry.numTokens; 
		}
		inputStream.close();
	}
	
	public static void setLengths(int totEntries, ShortDocDictInputStream inputStream){
		lengths = new int[totEntries];
		for (int i=0; i<totEntries; i++){
			ShortDocDictEntry ddEntry = inputStream.nextEntry();
			lengths[ddEntry.docid] = ddEntry.numTokens; 
		}
		inputStream.close();
	}
	
	public static final int getNumberOfTokens(int docid){
		return lengths[docid];
	}
}
