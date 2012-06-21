package com.ntnu.laika.distributed.util;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LinkedByteBuffer {
	
	//private static ConcurrentLinkedQueue<ByteBufferNode> buffers = new ConcurrentLinkedQueue<ByteBufferNode>();
	public static ConcurrentLinkedByteBufferStack buffers = new ConcurrentLinkedByteBufferStack();
	private LinkedByteBufferNode first, last; 
	private ByteBuffer curbuffer;
	private int size;
	private int numnodes;
	
	public LinkedByteBuffer(){
		first = buffers.poll();
		if (first == null) first = new LinkedByteBufferNode();
		last = first;
		curbuffer = last.buffer;
		numnodes = 1;
		size = 0;
	}
	
	public int getSize(){
		return size;
	}
	
	public void flip(){
		curbuffer.flip();
	}
	
	public void flushToChannelBufferAndFree(ChannelBuffer ch){
		if (first!=null) {
			LinkedByteBufferNode node = first;
			while (node != null){
				curbuffer = node.buffer;
				ch.writeBytes(curbuffer);
				curbuffer.clear();
				node = node.next;
			}
			
			buffers.add(first, last, numnodes);
			first = last = null;
			curbuffer = null;
			numnodes = size = 0;
		}
	}
	
	public void put(byte b){
		if (curbuffer.remaining() >= 1){
			curbuffer.put(b);
		} else {
			curbuffer.flip();
			LinkedByteBufferNode tmp = buffers.poll();
			if (tmp == null) tmp = new LinkedByteBufferNode();
			last.next = tmp;
			last = tmp;
			curbuffer = last.buffer;
			curbuffer.put(b);
			numnodes++;
		}
		size++;
	}

	public void putInt(int i) {
		if (curbuffer.remaining() >= 4){
			curbuffer.putInt(i);
		} else {
			curbuffer.flip();
			LinkedByteBufferNode tmp = buffers.poll();
			if (tmp == null) tmp = new LinkedByteBufferNode();
			last.next = tmp;
			last = tmp;
			curbuffer = last.buffer;
			curbuffer.putInt(i);
			numnodes++;
		}
		size+=4;
	}

	public void putInt(int[] input, int offset, int len) {
		for (int i=offset; i<offset+len; i++) putInt(input[i]);
	}

	public void putFloat(float f) {
		if (curbuffer.remaining() >= 4){
			curbuffer.putFloat(f);
		} else {
			curbuffer.flip();
			LinkedByteBufferNode tmp = buffers.poll();
			if (tmp == null) tmp = new LinkedByteBufferNode();
			last.next = tmp;
			last = tmp;
			curbuffer = last.buffer;
			curbuffer.putFloat(f);
			numnodes++;
		}
		size +=4;
	}
	
	//FIXME: test this method.
	
	public static void main(String args[]){
		float[] testdata = new float[1000000];
		for (int j=0; j<10000; j++){
			LinkedByteBuffer lb = new LinkedByteBuffer();
			for (int i=0; i<testdata.length; i++){
				testdata[i] = (float) Math.random();
				lb.putFloat(testdata[i]);
			}
			ChannelBuffer cb = ChannelBuffers.buffer(lb.size);
			lb.flip();
			lb.flushToChannelBufferAndFree(cb);
			for (int i=0; i<testdata.length; i++){
				if (cb.readFloat() != testdata[i]) System.out.println("gay");
			}
			System.out.println(" " + j);
		}
		System.out.println(buffers.size);
	}
}
