package com.ntnu.laika.structures;

import java.nio.ByteBuffer;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface BufferWrapper extends Closeable {
    public byte get();
    public void put(byte b);
    public void get(byte array[], int offset, int length);
    public void put(byte array[], int offset, int length);
    public int getInt();
    public void putInt(int i);
    public void getInt(int array[], int offset, int length);
    public void putInt(int array[], int offset, int length);
    public long getLong();
    public void putLong(long l);
    public void getLong(long array[], int offset, int length);
    public void putLong(int array[], int offset, int length);
    public void putDouble(double d);
    public double getDouble();
    public void get(ByteBuffer buffer, int length);
    public void put(ByteBuffer buffer, int length);
    public long position();
    public void position(long pos);
    public void forward(long pos);
	public void debug();
}
