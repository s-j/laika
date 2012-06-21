package com.ntnu.laika.distributed.util;

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LinkedByteBufferNode{
	public static final int SIZE = 32768;
	protected ByteBuffer buffer;
	protected LinkedByteBufferNode next = null;

	public LinkedByteBufferNode(){
		buffer = ByteBuffer.allocateDirect(SIZE);
	}
}