package com.ntnu.laika.distributed.util;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ConcurrentLinkedByteBufferStack{
	protected LinkedByteBufferNode first = null;
	protected int size;
	
	public synchronized LinkedByteBufferNode poll(){
		if (first != null) {
			LinkedByteBufferNode ret = first;
			first = ret.next;
			ret.next = null;
			size--;
			return ret;
		} else return null;
	}
	
	public synchronized void add(LinkedByteBufferNode node){
		node.next = first;
		first = node;
		size++;
	}
	
	public synchronized void add(LinkedByteBufferNode firstnode, LinkedByteBufferNode lastnode, int numnodes){
		lastnode.next = first;
		first = firstnode;
		size += numnodes;
	}
	public synchronized int size(){
		return size;
	}
}
