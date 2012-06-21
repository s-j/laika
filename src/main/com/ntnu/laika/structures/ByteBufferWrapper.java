package com.ntnu.laika.structures;

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ByteBufferWrapper implements BufferWrapper{
	private ByteBuffer bytebuffer;
	
	public ByteBufferWrapper(ByteBuffer bytebuffer){
		this.bytebuffer = bytebuffer;
	}
	
	@Override
	public void forward(long pos) {
		bytebuffer.position(bytebuffer.position()+(int)pos);
	}

	@Override
	public byte get() {
		return bytebuffer.get();
	}

	@Override
	public void get(byte[] array, int offset, int length) {
		bytebuffer.get(array, offset, length);
	}

	@Override
	public void get(ByteBuffer buffer, int length) {
		for (int i=0; i<length; i++) buffer.put(bytebuffer.get());
	}

	@Override
	public int getInt() {
		return bytebuffer.getInt();
	}

	@Override
	public void getInt(int[] array, int offset, int length) {
		for (int i=offset; i<offset+length; i++) array[i]=bytebuffer.getInt();
	}

	@Override
	public long getLong() {
		return bytebuffer.getLong();
	}

	@Override
	public void getLong(long[] array, int offset, int length) {
		for (int i=offset; i<offset+length; i++) array[i]=bytebuffer.getLong();
	}

	@Override
	public long position() {
		return bytebuffer.position();
	}

	@Override
	public void position(long pos) {
		bytebuffer.position((int)pos);
	}

	@Override
	public void put(byte b) {
		bytebuffer.put(b);
	}

	@Override
	public void put(byte[] array, int offset, int length) {
		bytebuffer.put(array, offset, length);
	}

	@Override
	public void put(ByteBuffer buffer, int length) {
		for (int i=0; i<length; i++) bytebuffer.put(buffer.get());
	}

	@Override
	public void putInt(int i) {
		bytebuffer.putInt(i);
	}

	@Override
	public void putInt(int[] array, int offset, int length) {
		for (int i=offset; i<offset+length; i++) bytebuffer.putInt(array[i]);
	}

	@Override
	public void putLong(long i) {
		bytebuffer.putLong(i);
	}

	@Override
	public void putLong(int[] array, int offset, int length) {
		for (int i=offset; i<offset+length; i++) bytebuffer.putLong(array[i]);
	}

	@Override
	public void close(){}

	@Override
	public double getDouble() {
		return bytebuffer.getDouble();
	}

	@Override
	public void putDouble(double d) {
		bytebuffer.putDouble(d);
	}
	
	@Override
	public void debug(){
	}
}
