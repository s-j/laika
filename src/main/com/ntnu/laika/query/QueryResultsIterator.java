package com.ntnu.laika.query;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QueryResultsIterator {
	protected QueryResults results;
	protected int idx;
	
	public QueryResultsIterator(QueryResults results){
		this.results = results;
		idx = -1;
	}
	
	public boolean next(){
		if (idx + 1 < results.numberOfResults){
			idx++;
			return true;
		} else {
			return false;
		}
	}
	
	public int getDocId(){
		return results.docids[idx];
	}
	
	public double getScore(){
		return results.scores[idx];
	}
}
