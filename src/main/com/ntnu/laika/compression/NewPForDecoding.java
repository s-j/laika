package com.ntnu.laika.compression;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.structures.BufferWrapper;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class NewPForDecoding {
	private final int padLimit;

	private final int[] intermediateBuffer = new int[128];
	private final int[] pforExceptions = new int[128 + 28];
	private final int[] pforExceptionIndexes = new int[128 + 28];

    public void decodeSingle(ChannelBuffer data, int count, int[] output, int outOffset){
    	if (count < padLimit){
    		decodeWithFallbackMechanism(data, count, output, outOffset);
    	} else {
    		int[] ctrwd = PForUtils.unpackControlWord(data.readInt());
    		int bitWidth = ctrwd[0], chunkLength = ctrwd[1], numberOfExceptions = ctrwd[2], frameOfReference = ctrwd[3];

	    	int len = bitWidth << 2;
	    	for (int i= 0; i < len; i++) intermediateBuffer[i] = data.readInt();

		    for (int i = 0; 
		    	i < numberOfExceptions;
		        i += Simple9.decode(data.readInt(), i, pforExceptions));
	
		    for (int i = 0;
		    	i < numberOfExceptions;
		        i += Simple9.decode(data.readInt(), i, pforExceptionIndexes));
	
		    NewPFor.decode(chunkLength, bitWidth, frameOfReference, numberOfExceptions, intermediateBuffer, output, pforExceptions, pforExceptionIndexes, 0, outOffset);
    	}
    }
	
	
    public void decodeSingle(ByteBuffer data, int count, int[] output, int outOffset){
    	if (count < padLimit){
    		decodeWithFallbackMechanism(data, count, output, outOffset);
    	} else {
    		int start = data.position();
    		IntBuffer intbuffer = data.asIntBuffer();
    	
    		int[] ctrwd = PForUtils.unpackControlWord(intbuffer.get());
    		int bitWidth = ctrwd[0], chunkLength = ctrwd[1], numberOfExceptions = ctrwd[2], frameOfReference = ctrwd[3];

	    	intbuffer.get(intermediateBuffer, 0, bitWidth << 2);
	    	

		    for (int i = 0; 
		    	i < numberOfExceptions;
		        i += Simple9.decode(intbuffer.get(), i, pforExceptions));
	
		    for (int i = 0;
		    	i < numberOfExceptions;
		        i += Simple9.decode(intbuffer.get(), i, pforExceptionIndexes));
	
		    NewPFor.decode(chunkLength, bitWidth, frameOfReference, numberOfExceptions, intermediateBuffer, output, pforExceptions, pforExceptionIndexes, 0, outOffset);
		    data.position(start + (intbuffer.position()<<2));
    	}
    }

    public void decodeSingle(BufferWrapper data, int count, int[] output, int outOffset){
    	if (count < padLimit){
    		decodeWithFallbackMechanism(data, count, output, outOffset);
    	} else {
    		int[] ctrwd = PForUtils.unpackControlWord(data.getInt());
    		int bitWidth = ctrwd[0], chunkLength = ctrwd[1], numberOfExceptions = ctrwd[2], frameOfReference = ctrwd[3];
	    	data.getInt(intermediateBuffer, 0, bitWidth << 2);
	    	try {
		    for (int i = 0; 
		    	i < numberOfExceptions;
		        i += Simple9.decode(data.getInt(), i, pforExceptions));
	
	    	} catch (ArrayIndexOutOfBoundsException ee){
	    		throw ee;
	    	}
	    	
		    for (int i = 0;
		    	i < numberOfExceptions;
		        i += Simple9.decode(data.getInt(), i, pforExceptionIndexes));
	
		    NewPFor.decode(chunkLength, bitWidth, frameOfReference, numberOfExceptions, intermediateBuffer, output, pforExceptions, pforExceptionIndexes, 0, outOffset);
    	}
    }

    public int padLimit(){
    	return padLimit;
    }

    public NewPForDecoding(){
   		this.padLimit = PForSettings.PadLimit;
    }
    
    public NewPForDecoding(int padLimit){
    	this.padLimit = padLimit;
	}
    
    
    private static int[] numValues = Simple9.numValues;
    
	public void skipSingle(ByteBuffer buffer, int count){
		if (count < padLimit){
			skipWithFallbackMechanism(buffer, count);
		} else {
			int start = buffer.position();
			IntBuffer intbuffer = buffer.asIntBuffer();
			int[] ctrwd = PForUtils.unpackControlWord(intbuffer.get());
	    	int bitWidth = ctrwd[0], numberOfExceptions = ctrwd[2]; //chunkLength = ctrwd[1], frameOfReference = ctrwd[3];
	    	
	    	intbuffer.position(1 + (bitWidth << 2)); //skip past ctrlword and packed data
	    	for (int i = 0; i < numberOfExceptions; i += numValues[buffer.get() >> 28]);
	    	for (int i = 0; i < numberOfExceptions; i += numValues[buffer.get() >> 28]);
	    	buffer.position(start + (intbuffer.position()<<2));
		}
	}

    public void SkipMultiple(ByteBuffer data, int blocks, int remaining){
    	int realBlocks = remaining >> 7;
	    int rest = remaining & 127;

	    if (realBlocks >= blocks){
	    	realBlocks = blocks;
	        rest = 0;
	    } else if (rest >= padLimit){
	    	realBlocks++;
	        rest = 0;
	    }

	    for (int i=0; i<realBlocks; i++){
	    	skipSingle(data, 128);
	    }
	   
	   if (rest > 0){
		   skipWithFallbackMechanism(data, rest);
	   }
    }

	private void skipWithFallbackMechanism(ByteBuffer data, int count){
		VByte.skipMany(count, data);
	}

	private void decodeWithFallbackMechanism(ChannelBuffer data, int count, int[] output, int outOffset){
		for (int i = 0; i < count; i++){
			output[outOffset++] = VByte.decode(data);
		}
	}
	
	private void decodeWithFallbackMechanism(ByteBuffer data, int count, int[] output, int outOffset){
		for (int i = 0; i < count; i++){
			output[outOffset++] = VByte.decode(data);
		}
	}
	
	private void decodeWithFallbackMechanism(BufferWrapper data, int count, int[] output, int outOffset){
		for (int i = 0; i < count; i++){
			output[outOffset++] = VByte.decode(data);
		}
	}
}