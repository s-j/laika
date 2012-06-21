package com.ntnu.laika.compression;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class PackUtils {
	/**
	 * packs width-compressed data from an array  
	 * @param width - bit width of a single element
	 * @param n - number of elements
	 * @param ioffset - start offset of the input
	 * @param ooffset - start offset of the output
	 * @param input - input array
	 * @param output - output array
	 * @return number of bits written to the output array
	 */
	//Note: the output should be clear for other data!
	public static int pack(int width, int n, int ioffset, int ooffset, int[] input, int[] output){
		if (32 % width == 0){
			int bitPosition = 0;
	        for (int i = 0; i < n; i++, bitPosition += width){
	        	int wordPosition = bitPosition >> 5;
	            int intraWordPosition = bitPosition & 0x1f;
	            output[ooffset+wordPosition] |= input[ioffset+i] << intraWordPosition;
	        }
	        return bitPosition;
		} else {
			int bitPosition = 0;
	        for (int i = 0; i < n; i++, bitPosition += width){
	        	int wordPosition = bitPosition >> 5;
	            int intraWordPosition = bitPosition & 0x1f;
	            int overflow = intraWordPosition + width - 32;
	            if (overflow <= 0){
	            	output[ooffset+wordPosition] |= input[ioffset+i] << intraWordPosition;
	            } else {
	            	output[wordPosition] |= input[i] << intraWordPosition;
	                output[wordPosition + 1] = input[i] >>> (width - overflow);
	            }
	        }
	        return bitPosition;
	    }
	}

	public static final int[] masks = {
	    0, 1, 3, 7, 0xf, 
	    0x1f, 0x3f, 0x7f, 0xff, 
	    0x1ff, 0x3ff, 0x7ff, 0xfff, 
	    0x1fff, 0x3fff, 0x7fff, 0xffff, 
	    0x1ffff, 0x3ffff, 0x7ffff, 0xfffff,
	    0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 
	    0x1ffffff, 0x3ffffff, 0x7ffffff, 0xfffffff,
	    0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff,
	};
	
	/**
	 * unpacks width-compressed data into an array  
	 * @param width - bit width of a single element
	 * @param n - number of elements
	 * @param ioffset - start offset of the input
	 * @param ooffset - start offset of the output
	 * @param input - input array
	 * @param output - output array
	 * @return number of bits read from the input array
	 */
	public static int unpack(int width, int n, int ioffset, int ooffset, int[] input, int[] output){
		int bit = 0;
		for (int i = 0, outputPos = ooffset; i < n; i++, outputPos++, bit += width){
			int word = ioffset + (bit >>> 5);
			int wordPos = bit - (word << 5);
	        int remainder = 32 - width - wordPos;
	        if (remainder >= 0){   // inside
	        	output[outputPos] = (input[word] >>> wordPos) & masks[width];
	        } else {   // spanning
	        	remainder = -1 * remainder;
	            output[outputPos] = (input[word] >>> wordPos) & masks[width];
	            output[outputPos] |= (input[word + 1] & (masks[width] >>> (width - remainder))) << (width - remainder);
	        }
		}
		return bit;
	}
	
	public static void cleanArray(int[] arr){
		for (int i=0; i<arr.length; i++) arr[i] = 0;
	}
	
	public static void main(String[] args){
		int a[] = {0, 4, 3, 2, 1, 5, 7, 8 , 6};
		int b[] = new int[4];
		int c[] = new int[9];
		
		pack(4, 9, 0, 0, a, b);
		unpack(4, 9, 0, 0, b, c);
		
		for (int i=0; i<9; i++) System.out.println(c[i]+" ");
	}
}

