package com.ntnu.laika.utils;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class BitUtils {
    private static final int[] multiplyDeBruijnBitPosition =  {
    	0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8, 
    	31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
    };
    
   
	public static int MSB(int number){	
        number |= number >>> 1;
        number |= number >>> 2;
        number |= number >>> 4;
        number |= number >>> 8;
        number |= number >>> 16;
        number = (number >>> 1) + 1;
        return multiplyDeBruijnBitPosition[(number * 0x077CB531) >>> 27];
	}

    public static int abs(int number){
        int temp = number >> 31;
        return (number ^ temp) - temp;
    }

	public static int min(int a, int b) {
        return (a + b - abs((int)a - (int)b)) >> 1;
	}
	
	public static int max(int a, int b) {
        return (a + b + abs((int)a - (int)b)) >> 1;
	}
}