package com.ntnu.laika.query.processing;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.processing.scoremodels.BM25;
import com.ntnu.laika.query.processing.scoremodels.WeightingModel;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public abstract class QueryProcessing {
	
	protected DiskInvertedIndex index;
	protected ResultHeap rheap;
	protected WeightingModel wmodel;
	protected Statistics stats;
	
	public long accScoreTime = 0l, procLoopTime = 0l;
	public int numQueries = 0;
	
	public QueryProcessing(DiskInvertedIndex index, Statistics stats){
		this.index = index;
		wmodel = new BM25();
		wmodel.setAverageDocumentLength(stats.getAverageDocumentLength());
		wmodel.setNumberOfDocuments(stats.getNumberOfDocuments());
		this.stats = stats;
		
		rheap = new ResultHeap(Constants.MAX_NUMBER_OF_RESULTS);
	}
	
	public abstract QueryResults processQuery(Query query);	
}
