package com.ntnu.laika.distributed.util;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ConcurrentLinkedArrayStack{
	protected LinkedArrayNode first = null;
	protected int size;
	
	public synchronized LinkedArrayNode poll(){
		if (first != null) {
			LinkedArrayNode ret = first;
			first = ret.next;
			ret.next = null;
			size--;
			return ret;
		} else return null;
	}
	
	public synchronized void add(LinkedArrayNode node){
		node.next = first;
		first = node;
		size++;
	}
	
	public synchronized void add(LinkedArrayNode firstnode, LinkedArrayNode lastnode, int numnodes){
		lastnode.next = first;
		first = firstnode;
		size += numnodes;
	}
	
	public synchronized int size(){
		return size;
	}
}
