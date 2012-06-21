package com.ntnu.laika.compression;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import com.ntnu.laika.distributed.util.LinkedByteBuffer;
import com.ntnu.laika.utils.BitUtils;


/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class NewPForEncoding {

		private final int[] intermediateBuffer = new int[128];
        private final int[] outputBuffer = new int[128];
        private final int[] exceptions = new int[128];
        private final int[] exceptionIndexes = new int[128];
        private final int[] simple9Buffer = new int[256];

        private int padLimit;

        public int PadLimit(){
            return padLimit;
        }

        public NewPForEncoding(){
        	 padLimit = PForSettings.PadLimit;
        }
        
        public NewPForEncoding(int padLimit){
            this.padLimit = padLimit;
        }

        public void encodeMultiple(ByteBuffer buffer, int[] values, int count){
            for (int i = 0; i < count; i += 128){
                encodeSingle(buffer, values, i, BitUtils.min(128, count - i));    
            }
        }
        public void encodeSingle(LinkedByteBuffer buffer, int[] values, int valuesOffset, int count){
            if (count < padLimit){
                encodeWithFallbackMechanism(buffer, values, valuesOffset, count);
            } else {
	            int frame = PForUtils.minimumFoR(count, values, valuesOffset);
	            
	            if (count < 128){
	            	if (valuesOffset + 128 < values.length){
	            		padRemainingSlots(frame, count, valuesOffset, values);
	            	} else {
	            		values = copyAndPadRemainingSlots(frame, count, valuesOffset, values);
	            		valuesOffset = 0;
	            	}
	            }
	            
	            int width = PForUtils.log2HistogramAnalyze(PForSettings.InRangeRequirement, frame, values, valuesOffset);
	            clearOutput();
	            
	            int numException = NewPFor.encode(
	                width, frame, values, intermediateBuffer, valuesOffset, outputBuffer, exceptionIndexes, exceptions);
	
	            
	            
	            int outputWord = 0;
	            for (int i = 0;
	                 i < numException;
	                 i += Simple9.encode(i, outputWord, exceptions, simple9Buffer), outputWord++);
	            
	            for (int i = 0;
	                 i < numException;
	                 i += Simple9.encode(i, outputWord, exceptionIndexes, simple9Buffer), outputWord++);
	
	            //int codeBytes = width << 4;
	            
	            buffer.putInt(PForUtils.createControlWord(width, numException, frame));
	            buffer.putInt(outputBuffer, 0, width<<2);
	            buffer.putInt(simple9Buffer, 0, outputWord);
	            //System.out.println(buffer.position());
            }
        }
        
        public void encodeSingle(ByteBuffer buffer, int[] values, int valuesOffset, int count){
            if (count < padLimit){
                encodeWithFallbackMechanism(buffer, values, valuesOffset, count);
            } else {
	            int frame = PForUtils.minimumFoR(count, values, valuesOffset);
	            
	            if (count < 128){
	            	if (valuesOffset + 128 < values.length){
	            		padRemainingSlots(frame, count, valuesOffset, values);
	            	} else {
	            		values = copyAndPadRemainingSlots(frame, count, valuesOffset, values);
	            		valuesOffset = 0;
	            	}
	            }
	            
	            int width = PForUtils.log2HistogramAnalyze(PForSettings.InRangeRequirement, frame, values, valuesOffset);
	            clearOutput();
	            
	            int numException = NewPFor.encode(
	                width, frame, values, intermediateBuffer, valuesOffset, outputBuffer, exceptionIndexes, exceptions);
	
	            
	            
	            int outputWord = 0;
	            for (int i = 0;
	                 i < numException;
	                 i += Simple9.encode(i, outputWord, exceptions, simple9Buffer), outputWord++);
	            
	            for (int i = 0;
	                 i < numException;
	                 i += Simple9.encode(i, outputWord, exceptionIndexes, simple9Buffer), outputWord++);
	
	            //int codeBytes = width << 4;
	            
	            int start = buffer.position();
	            IntBuffer intbuffer = buffer.asIntBuffer();
	            
	            intbuffer.put(PForUtils.createControlWord(width, numException, frame));
	            intbuffer.put(outputBuffer, 0, width<<2);
	            intbuffer.put(simple9Buffer, 0, outputWord);
	            
	            buffer.position(start + (intbuffer.position() << 2));
	            //System.out.println(buffer.position());
            }
        }

        private void encodeWithFallbackMechanism(LinkedByteBuffer buffer, int[] values, int valuesOffset, int count){
            for (int i = valuesOffset; i < valuesOffset + count; i++){
                VByte.encode(values[i], buffer);
            }
        }
        
        private void encodeWithFallbackMechanism(ByteBuffer buffer, int[] values, int valuesOffset, int count){
            for (int i = valuesOffset; i < valuesOffset + count; i++){
                VByte.encode(values[i], buffer);
            }
        }

        private void padRemainingSlots(int frameOfReference, int startAt, int valuesOffset, int[] values){
        	int startPad = valuesOffset+startAt;
        	for (int i = startPad; i < startPad+128; i++){
                values[i] = frameOfReference;
            }
        }

        private int[] copyAndPadRemainingSlots(int frameOfReference, int startAt, int valuesOffset, int[] values){
            int out[] = new int[128];
            for (int i = 0; i < startAt; i++) out[i] = values[valuesOffset+i];
        	for (int i = startAt; i < 128; i++) out[i] = frameOfReference;
        	return out;
        }
        
        private void clearOutput(){
        	for (int i=0; i<128; i++){
        		outputBuffer[i] = 0;
        	}
        }
}
