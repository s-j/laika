package com.ntnu.laika.compression;

import com.ntnu.laika.utils.BitUtils;


/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class PForUtils {
	public static int log2HistogramAnalyze(int inRange, int FoR, int[] data, int offset){
		int[] hist = new int[33];
		for (int i=0; i<128; i++) hist[BitUtils.MSB(data[offset + i] - FoR)+1]++;	
		
		int acc = 0;
		for (int i=0; i<32; i++) if ((acc += hist[i]) >= inRange) return i; 
		
		return 32;
	}
	
	public static int minimumFoR(int cnt, int[] data, int offset){
		int min = data[offset];
		for (int i=offset + 1; i<cnt + offset; i++){
			min = BitUtils.min(min, data[i]);
		}
		return min & 0x7ffff;
	}
	
	public static int[] unpackControlWord(int word){
		int[] ret = new int[4];
        // 5 bit bit width, 7 bit exceptions, 20 bit frameOfReference. Chunk length is allways 128.
		ret[0] = word >>> 27;			ret[1] = 128;
		ret[2] = (word >>> 19) & 0xff;	ret[3] = word & 0x7ffff;
		return ret;
	}
	
	public static int createControlWord(int width, int numExceptions, int FoR){
        // 5 bit bit width, 7 bit exceptions, 20 bit frameOfReference. Chunk length is allways 128.
		return ((width & 0x1f) << 27) | ((numExceptions & 0xff) << 19) | (FoR & 0x7ffff);
	}
}