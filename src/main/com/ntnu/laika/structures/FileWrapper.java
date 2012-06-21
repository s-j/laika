package com.ntnu.laika.structures;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ntnu.laika.Constants;
import com.ntnu.laika.buffering.Buffer;
import com.ntnu.laika.buffering.BufferPool;
import com.ntnu.laika.buffering.FileBlockPointer;
import com.ntnu.laika.runstats.SimpleStats;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FileWrapper implements BufferWrapper{
	protected BufferPool bufferPool;
	protected int fileNumber;
	protected Buffer currentBuffer;
	boolean currentDirty = false;
	protected ByteBuffer activeByteBuffer;
	
	protected long startBlockOffset;
	protected long blockOffset;
	protected int startByteOffset;
	protected int byteOffset;
	
    public FileWrapper(BufferPool bufferPool, int fileNumber)
    		throws IOException, InterruptedException {
        this(bufferPool, fileNumber, 0, 0);
    }
	
    public FileWrapper(BufferPool bufferPool, int fileNumber, long position) 
    		throws IOException, InterruptedException {
    	   
    	this.bufferPool = bufferPool;
        this.fileNumber = fileNumber;
        
        this.blockOffset = startBlockOffset = position / Constants.BUFFER_BLOCK_SIZE;
        this.byteOffset = startByteOffset = (int) (position - startBlockOffset * Constants.BUFFER_BLOCK_SIZE);
           
		//SimpleStats.addDescription(0, 1);
        currentBuffer = bufferPool.pinBuffer(new FileBlockPointer(fileNumber, blockOffset));
           
        activeByteBuffer = currentBuffer.getByteBuffer();
        activeByteBuffer.position(byteOffset);
    }
	
    public FileWrapper(BufferPool bufferPool, int fileNumber, long blockOffset, int byteOffset)
    		throws IOException, InterruptedException {
        this.bufferPool = bufferPool;
        this.fileNumber = fileNumber;
        
        currentBuffer = bufferPool.pinBuffer(new FileBlockPointer(fileNumber, blockOffset));
        
        activeByteBuffer = currentBuffer.getByteBuffer();
        activeByteBuffer.position(byteOffset);
        
        this.blockOffset = startBlockOffset = blockOffset;
        this.byteOffset = startByteOffset = byteOffset;
    }
    
    
    
    
    /**
     * Get byte wrapper
     * @return next byte
     */
    public byte get() {
		if (activeByteBuffer.remaining() < 1) {
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
		byteOffset++;
		return activeByteBuffer.get();
    }    
    
    public void put(byte b) {
		if (activeByteBuffer.remaining() < 1) {
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
		activeByteBuffer.put(b);
		byteOffset++;
		currentDirty |= true;
    }  
    
    public void get(byte array[], int offset, int length) {
   		while (length > activeByteBuffer.remaining()){
   			int rem = activeByteBuffer.remaining();
   			activeByteBuffer.get(array, offset, rem);
   			length -= rem; offset += rem;
   			try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
   		}
   		byteOffset += length;
 		activeByteBuffer.get(array, offset, length);
    }

    public void put(byte array[], int offset, int length) {
   		while (length > activeByteBuffer.remaining()){
   			int rem = activeByteBuffer.remaining();
   			activeByteBuffer.put(array, offset, rem);
   			currentDirty |= true;
   			length -= rem; offset += rem;
   			try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
   		}
 		activeByteBuffer.put(array, offset, length);
 		byteOffset += length;
 		currentDirty |= true;
    }
    
    public int getInt() {
		if (activeByteBuffer.remaining() > Constants.INT_SIZE) {
			byteOffset += 4;
			return activeByteBuffer.getInt();
        } else if (activeByteBuffer.remaining() == 0){
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			byteOffset += 4;
			return activeByteBuffer.getInt();
        } else {
        	int ret = (get() & 0xff) << 24;
        	ret |= (get() & 0xff) << 16;
    		ret |= (get() & 0xff) << 8;
    		ret |= (get() & 0xff);
    		return ret;
        }
    }
    
    public void putInt(int i){
		if (activeByteBuffer.remaining() > Constants.INT_SIZE) {
			byteOffset += 4;
			activeByteBuffer.putInt(i);
			currentDirty = true;
        } else if (activeByteBuffer.remaining() == 0){
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			byteOffset += 4;
			activeByteBuffer.putInt(i);
			currentDirty = true;
        } else {
        	put((byte)(i>>>24));
        	put((byte)(0xff & (i>>>16)));
        	put((byte)(0xff & (i>>>8)));
        	put((byte)(0xff & i));
        }
    }
    
    public void getInt(int array[], int offset, int length) {
		for (int i=offset; i<offset+length; i++) array[i]=getInt();
    }

    public void putInt(int array[], int offset, int length) {
		for (int i=offset; i<offset+length; i++) putInt(array[i]);
    }
    
    public long getLong() {
		if (activeByteBuffer.remaining() > Constants.LONG_SIZE) {
			byteOffset += 8;
			return activeByteBuffer.getLong();
        } else if (activeByteBuffer.remaining() == 0){
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			byteOffset += 8;
			return activeByteBuffer.getLong();
        } else {
        	long ret = (get() & 0xffL) << 56;
        	ret |= (get() & 0xffL) << 48;
    		ret |= (get() & 0xffL) << 40;
    		ret |= (get() & 0xffL) << 32;
    		
    		ret |= (get() & 0xffL) << 24;
    		ret |= (get() & 0xffL) << 16;
    		ret |= (get() & 0xffL) << 8;
    		ret |= (get() & 0xffL);
    		return ret;
        }
    }
    public void putLong(long l){
		if (activeByteBuffer.remaining() > Constants.LONG_SIZE) {
			byteOffset += 8;
			activeByteBuffer.putLong(l);
			currentDirty = true;
        } else if (activeByteBuffer.remaining() == 0){
        	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			byteOffset += 8;
			activeByteBuffer.putLong(l);
			currentDirty = true;
        } else {
        	put((byte)(l>>>56));
        	put((byte)(0xff & (l>>>48)));
        	put((byte)(0xff & (l>>>40)));
        	put((byte)(0xff & (l>>>32)));
        	
        	put((byte)(0xff & (l>>>24)));
        	put((byte)(0xff & (l>>>16)));
        	put((byte)(0xff & (l>>>8)));
        	put((byte)(0xff & l));
        }
    }

    public void getLong(long array[], int offset, int length) {
		for (int i=offset; i<offset+length; i++) array[i]=getLong();
    }

    public void putLong(int array[], int offset, int length) {
		for (int i=offset; i<offset+length; i++) putLong(array[i]);
    }
    
    
    
    
    public void get(ByteBuffer buffer, int length){
    	while (activeByteBuffer.remaining() < length) {
           	int rem = activeByteBuffer.remaining();
           	for (int i=0; i<rem; i++) buffer.put(activeByteBuffer.get());
           	length -= rem;
           	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
         }
         for (int i=0; i<length; i++) buffer.put(activeByteBuffer.get());
         byteOffset += length; 
    }
    
    public void put(ByteBuffer buffer, int length){
    	//System.out.println(length + " " + activeByteBuffer.remaining());
    	while (activeByteBuffer.remaining() < length) {
           	int rem = activeByteBuffer.remaining();
           	for (int i=0; i<rem; i++) activeByteBuffer.put(buffer.get());
           	length -= rem;
           	currentDirty |= true;
           	try {
				moveNextBlock();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
         }
            
         for (int i=0; i<length; i++) activeByteBuffer.put(buffer.get());
         byteOffset += length;
         currentDirty |= true;
    }
    
    
    
    public long position() {
    	//System.out.println(blockOffset + " " + startBlockOffset + " " + byteOffset + " " + startByteOffset);
    	return  (blockOffset - startBlockOffset) * Constants.BUFFER_BLOCK_SIZE + (byteOffset-startByteOffset) ;
    }
    
    public void position(long pos){
    	pos += startByteOffset;
    	long blocksOff = pos / Constants.BUFFER_BLOCK_SIZE;
    	
    	try {
			moveRandomBlock(startBlockOffset + blocksOff);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		byteOffset = (int)(pos - blocksOff * Constants.BUFFER_BLOCK_SIZE);
		activeByteBuffer.position(byteOffset);
    }
    
    public void forward(long pos){
    	long offblocks = pos / Constants.BUFFER_BLOCK_SIZE;
    	int newpos = (int)(pos - offblocks * Constants.BUFFER_BLOCK_SIZE) + byteOffset;
    	
    	if (pos > Constants.BUFFER_BLOCK_SIZE){
    		offblocks++; pos -= Constants.BUFFER_BLOCK_SIZE;
    	}
    	
    	try {
    		moveRandomBlock(blockOffset + offblocks);
			activeByteBuffer.position(newpos);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
	
    
    private void moveNextBlock() throws IOException, InterruptedException {
        FileBlockPointer next = currentBuffer.getBlockPointer().next();
        currentBuffer.setIsDirty(currentDirty);
        bufferPool.unPinBuffer(currentBuffer);
        
		//SimpleStats.addDescription(0, 1);
        currentBuffer = bufferPool.pinBuffer(next);
        activeByteBuffer = currentBuffer.getByteBuffer();
        blockOffset++; byteOffset = 0;
        activeByteBuffer.position(0);
        currentDirty = false;
    }
    
    private void moveRandomBlock(long blockno) throws IOException, InterruptedException {
    	if (currentBuffer.getBlockPointer().getBlockNumber() == blockno) return;
    	
        currentBuffer.setIsDirty(currentDirty);
        bufferPool.unPinBuffer(currentBuffer);
		
        //SimpleStats.addDescription(0, 1);
        currentBuffer = bufferPool.pinBuffer(new FileBlockPointer(fileNumber, blockno));
        activeByteBuffer = currentBuffer.getByteBuffer();
        blockOffset = blockno; byteOffset = 0;
        activeByteBuffer.position(0);
        currentDirty = false;
    }

    
    public void close() {
    	if (currentBuffer != null) {
    		currentBuffer.setIsDirty(currentDirty);
    		bufferPool.unPinBuffer(currentBuffer);
    		currentBuffer = null;
    	}
    	activeByteBuffer = null;
    	bufferPool = null; // make it impossible to pin buffers
    }
    
	@Override
	public double getDouble() {
		return Double.longBitsToDouble(getLong());
	}

	@Override
	public void putDouble(double d) {
		putLong(Double.doubleToRawLongBits(d));
	}
   /* 
    public static void main(String args[]){
    	long l = Long.MAX_VALUE/3;
    	
    	byte a = (byte)(l>>>56);
    	byte b = (byte)(0xff & (l>>>48));
    	byte c = (byte)(0xff & (l>>>40));
    	byte d = (byte)(0xff & (l>>>32));
    	byte e = (byte)(0xff & (l>>>24));
    	byte f = (byte)(0xff & (l>>>16));
    	byte g = (byte)(0xff & (l>>>8));
    	byte h = (byte)(0xff & l);
    	
    	
    	long ret = (a & 0xffL) << 56;
    	ret |= (b & 0xffL) << 48;
    	ret |= (c & 0xffL) << 40;
    	ret |= (d & 0xffL) << 32;
    	ret |= (e & 0xffL) << 24;
    	ret |= (f & 0xffL) << 16;
    	ret |= (g & 0xffL) << 8;
    	ret |= (h & 0xffL);

    	System.out.println(l + " " + ret);
    }
    */
	@Override
	public void debug(){
		System.out.println(activeByteBuffer.remaining());
	}
}
