package com.ntnu.laika.query.processing;

import com.ntnu.laika.utils.Pair;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ResultHeap{
	private int[] docids;
	private double[] scores;
	
	private int heapsize;
	private int maxheapsize;
	private boolean sorted = false;
	
	public ResultHeap(int maxheapsize){
		this.docids = new int[maxheapsize];
		this.scores = new double[maxheapsize];
		this.heapsize=0;
		this.maxheapsize = maxheapsize;
	}
	
	private void buildHeap(){
		for (int i=(heapsize>>1)-1; i>=0; i--) heapify(i);
		sorted = true;
	}
	
	private void heapify(int p){
		int l, r, n, p2 = p<<1;
		l = p2+1;
		r = p2+2;

		double tscore;
		int tdocid;
		while (l < heapsize) {
			n = (r >= heapsize) ? l : ((scores[l] < scores[r]) ? l : r);	
			if (scores[p] > scores[n] || (scores[p] == scores[n] && docids[p] > docids[n])) {
				tscore = scores[p];
				tdocid = docids[p];
				scores[p] = scores[n];
				docids[p] = docids[n];
				scores[n] = tscore;
				docids[n] = tdocid;				
			} else {
				return;
			}
			p = n;
			p2 = p<<1;
			l = p2+1;
			r = p2+2;
		}
	}
	
	public boolean insertIfGreaterThanLeast(int docid, double score){
		if (heapsize<maxheapsize){
			//SimpleStats.addDescription(0, 1, true);
			docids[heapsize] = docid;
			scores[heapsize] = score;
			heapsize++;
			if (heapsize == maxheapsize) buildHeap();
			return true;
		} else {
			if (score > scores[0]){
				//SimpleStats.addDescription(0, 1, true);
				docids[0] = docid;
				scores[0] = score;
				heapify(0);
				return true;
			}
		}
		return false;
	}
	
	public final int size(){
		return heapsize;
	}
	
	public void decrSortResults(int resdocid[], double resscore[]){
		if (!sorted) buildHeap();
		while (heapsize-->0){
			resdocid[heapsize] = docids[0];
			resscore[heapsize] = scores[0];
			docids[0] = docids[heapsize];
			scores[0] = scores[heapsize];
			heapify(0);
		}
	}
	
	public final double minScore(){
		if (heapsize < maxheapsize) return 0.0d;
		else return scores[0];
	}
	
	public final double maxScore(){
		double max = scores[0];
		for (int i=1; i<heapsize; i++)
			max = max < scores[i] ? scores[i] : max; 
		return max;
	}
	
	public void reset(){
		heapsize = 0;
		sorted = false;
	}
	
	public Pair<int[],double[]> getRawData(){
		return new Pair<int[],double[]>(docids, scores);
	}
}