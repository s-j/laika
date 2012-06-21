package com.ntnu.laika.query;

import com.ntnu.laika.utils.BitUtils;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Query {
	protected QueryEntry qEntries[];
	protected int maxKeyFrequency = 0;
	            
	public Query(QueryEntry qEntries[]){
		this.qEntries = qEntries;
		for (QueryEntry qEntry : qEntries) 
			maxKeyFrequency = BitUtils.max(qEntry.keyFrequency, maxKeyFrequency);
	}
	
	public final int getNumberOfTerms(){
		return qEntries.length;
	}
	
	public final QueryEntry[] getEntries(){
		return qEntries;
	}
	
	public final int getMaxKeyFrequency(){
		return maxKeyFrequency;
	}
	
	public String toString(){
		StringBuffer b = new StringBuffer();
		for (QueryEntry qEntry : qEntries){
			b.append(qEntry+"\n");
		}
		return b.toString();
	}
}
