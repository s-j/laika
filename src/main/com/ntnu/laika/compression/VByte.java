package com.ntnu.laika.compression;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import com.ntnu.laika.distributed.util.LinkedByteBuffer;
import com.ntnu.laika.structures.BufferWrapper;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class VByte{
	
	public static void encode(int value, LinkedByteBuffer buffer) {
        while (value > 127){
            buffer.put((byte) ((value & 127) | 128));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }
	
	
	public static void encode(int value, ByteBuffer buffer) {
        while (value > 127){
            buffer.put((byte) ((value & 127) | 128));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }
	
	public static int decode(ByteBuffer buffer){
        int value = 0;
        int shift = 0;
        byte temp;
        do {  temp = buffer.get();
            value += ((temp & 127) << shift);
            shift += 7;
        } while (temp < 0);
        return value;
	}
	
	public static int decode(ChannelBuffer buffer){
        int value = 0;
        int shift = 0;
        byte temp;
        do {  temp = buffer.readByte();
            value += ((temp & 127) << shift);
            shift += 7;
        } while (temp < 0);
        return value;
	}
	
	
	public static int decode(BufferWrapper data){
        int value = 0;
        int shift = 0;
        byte temp;
        do {  temp = data.get();
            value += ((temp & 127) << shift);
            shift += 7;
        } while (temp < 0);
        return value;
	}
	
	
	public static void skipOne(ByteBuffer buffer){
		byte temp = buffer.get();
		while (temp < 0){
			temp = buffer.get();
		}
	}
	
	public static void skipMany(int n, ByteBuffer buffer){
		 while (n > 31){
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
			 n -= 32;
         }

		 while (n > 15){
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
             n -= 16;
         }

		 while (n > 7){
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
			 n -= 8;
         }

         while (n > 3){
        	 skipOne(buffer); skipOne(buffer); skipOne(buffer); skipOne(buffer);
			 
        	 n -= 4;
         }

         while (n > 0){
        	 skipOne(buffer);
             n--;
         }
	}
	
	public static void main(String args[]){
		ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
		
		encode(128, buffer);
		
		buffer.position(0);
		
		System.out.println(decode(buffer));
	}
}