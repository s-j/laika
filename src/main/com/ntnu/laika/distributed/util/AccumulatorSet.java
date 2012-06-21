package com.ntnu.laika.distributed.util;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.ntnu.laika.structures.postinglist.PostingListIterator;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class AccumulatorSet implements PostingListIterator{
	//private static ConcurrentLinkedQueue<LinkedArray> arrays = new ConcurrentLinkedQueue<LinkedArray>();
	public static ConcurrentLinkedArrayStack arrays = new ConcurrentLinkedArrayStack();
		
	//TODO: can be improved by introducing a MAXFREECOUNT and deleting arrays when there are too many free arrays.
	
	private int first_pos, last_pos, size;
	private LinkedArrayNode first, last;
	private int numnodes;
	
	public AccumulatorSet(){
		first = arrays.poll();
		if (first == null) first = new LinkedArrayNode();
		last = first;
		numnodes = 1;
		first_pos = last_pos = size = 0;
	}
	
	public AccumulatorSet(int[] docids, double[] scores, int spos, int fpos) {
		this();
		for (int i=spos; i<fpos; i++) addLast(docids[i],scores[i]);
	}

	public final boolean addLast(int i, double d){
		if (last_pos == LinkedArrayNode.STDCAPACITY) {
			LinkedArrayNode t = arrays.poll();
			if (t==null) t = new LinkedArrayNode();
			last.next = t;
			last = t;
			last_pos = 0;
			numnodes++;
		}
		last.iarray[last_pos] = i;
		last.darray[last_pos++] = d;
		size++;
//		SimpleStats.addDescription(2, 1, true);
		return true;
	}
	
	
	public final int getDocId(){
		return first.iarray[first_pos];
	}
	
	public final double getScore(){
		return first.darray[first_pos];
	}
	
	public boolean next(){
		if (size > 1 && first_pos < LinkedArrayNode.STDCAPACITY - 1) {
			size--;
			first_pos++;
			return true;
		} else if (size == 0) {
			return false;
		} else if (size == 1) {
			first_pos = last_pos = size = 0;
			return false;
		} else {
			LinkedArrayNode freenode = first;
			first = first.next;
			first_pos = 0;
			arrays.add(freenode);
			numnodes--;
			size--;
			return true;
		}
	}
	
	public boolean hasMore(){
		return size > 0;
	}
	
	public int size() {
		return size;
	}
	
	public void close() {
		if (first!=null) arrays.add(first, last, numnodes);
		first_pos = last_pos = size = numnodes = 0;
		first = last = null;
		/*LinkedArrayNode freenode;
		while (first != null) {
			freenode = first;
			first = first.next;
			freenode.clear();
			arrays.add(freenode);
		}
		last = null;*/
	}
	
	public boolean skipTo(int docid) {
		while (size > 0){
			if (first.iarray[first_pos] >= docid) {
				return true;
			} else{
				next();
			}
		}
		return false;
	}
	
	public static void main(String args[]){
		double[] testdata = new double[500000];
		for (int j=0; j<1000; j++){
			AccumulatorSet acc = new AccumulatorSet();
			for (int i=0; i<testdata.length; i++){
				testdata[i] = (float) Math.random();
				acc.addLast(i,testdata[i]);
			}
			for (int i=0; i<testdata.length; i++){
				if (acc.getScore() != testdata[i]) System.out.println("err");
				acc.next();
			}
			acc.close();
		}
		System.out.println(arrays.size());
	}

	@Override
	public int getFrequency() {
		throw new NotImplementedException();
	}
}
