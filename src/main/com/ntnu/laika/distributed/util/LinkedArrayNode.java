package com.ntnu.laika.distributed.util;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LinkedArrayNode{
	public static final int STDCAPACITY = 20000;
	protected int[] iarray;
	protected double[] darray;
	protected LinkedArrayNode next;
	//public static AtomicInteger numcreated = new AtomicInteger();
	
	public LinkedArrayNode(){
		iarray = new int[STDCAPACITY];
		darray = new double[STDCAPACITY];
		next = null;
	//	numcreated.incrementAndGet();
	}
	
	public void clear(){
		next = null;
	}
}
