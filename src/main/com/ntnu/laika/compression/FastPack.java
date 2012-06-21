package com.ntnu.laika.compression;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class FastPack {
	public static void fastNPack(int width, int n, int ioffset, int ooffset, int[] input, int[] output){
		while (n >= 128){
			fast128Pack(width, ioffset, ooffset, input, output);
			ioffset += 128;
	        ooffset += width << 2;
	        n -= 128;
		}

		while (n >= 32){
			fast32Pack(width, ioffset, ooffset, input, output);
            ioffset += 32;
	        ooffset += width;
	        n -= 32;
	    }

	    if (n > 0){
	    	PackUtils.pack(width, n, ioffset, ooffset, input, output);
	    }
	}

	public static void fastNUnPack(int width, int n, int ioffset, int ooffset, int[] input, int[] output){
		while (n >= 128){
			fast128UnPack(width, ioffset, ooffset, input, output);
			ooffset += 128;
			ioffset += width << 2;
			n -= 128;
		}

		while (n >= 32){
			fast32UnPack(width, ioffset, ooffset, input, output);
	        ooffset += 32;
	        ioffset += width;
	        n -= 32;
		}

		if (n > 0){
			PackUtils.unpack(width, n, ioffset, ooffset, input, output);
        }
	}

	public static void fast128Pack(int width, int ioffset, int ooffset, int[] input, int[] output){
		switch (width){
			case 0: 
				return;
			case 1: 
				fast32Pack1(ioffset, ooffset, input, output);
	            fast32Pack1(ioffset + 32, ooffset + 1, input, output);
	            fast32Pack1(ioffset + 64, ooffset + 2, input, output);
	            fast32Pack1(ioffset + 96, ooffset + 3, input, output);
	            return;
			case 2:
				fast32Pack2(ioffset, ooffset, input, output);
	            fast32Pack2(ioffset + 32, ooffset + 2, input, output);
	            fast32Pack2(ioffset + 64, ooffset + 4, input, output);
	            fast32Pack2(ioffset + 96, ooffset + 6, input, output);
                return;
			case 3:
				fast32Pack3(ioffset, ooffset, input, output);
	            fast32Pack3(ioffset + 32, ooffset + 3, input, output);
	            fast32Pack3(ioffset + 64, ooffset + 6, input, output);
	            fast32Pack3(ioffset + 96, ooffset + 9, input, output);
	            return;
			case 4:
				fast32Pack4(ioffset, ooffset, input, output);
	            fast32Pack4(ioffset + 32, ooffset + 4, input, output);
	            fast32Pack4(ioffset + 64, ooffset + 8, input, output);
	            fast32Pack4(ioffset + 96, ooffset + 12, input, output);
	            return;
			case 5:
				fast32Pack5(ioffset, ooffset, input, output);
	            fast32Pack5(ioffset + 32, ooffset + 5, input, output);
	            fast32Pack5(ioffset + 64, ooffset + 10, input, output);
	            fast32Pack5(ioffset + 96, ooffset + 15, input, output);
	            return;
			case 6:
				fast32Pack6(ioffset, ooffset, input, output);
	            fast32Pack6(ioffset + 32, ooffset + 6, input, output);
	            fast32Pack6(ioffset + 64, ooffset + 12, input, output);
	            fast32Pack6(ioffset + 96, ooffset + 18, input, output);
	            return;
			case 7:
				fast32Pack7(ioffset, ooffset, input, output);
	            fast32Pack7(ioffset + 32, ooffset + 7, input, output);
	            fast32Pack7(ioffset + 64, ooffset + 14, input, output);
	            fast32Pack7(ioffset + 96, ooffset + 21, input, output);
	            return;
			case 8:
	          	fast32Pack8(ioffset, ooffset, input, output);
	    	    fast32Pack8(ioffset + 32, ooffset + 8, input, output);
	    	    fast32Pack8(ioffset + 64, ooffset + 16, input, output);
	    	    fast32Pack8(ioffset + 96, ooffset + 24, input, output);
	    	    return;
			case 9:
				fast32Pack9(ioffset, ooffset, input, output);
	            fast32Pack9(ioffset + 32, ooffset + 9, input, output);
	            fast32Pack9(ioffset + 64, ooffset + 18, input, output);
	            fast32Pack9(ioffset + 96, ooffset + 27, input, output);
	            return;
			default:
	            PackUtils.pack(width, 128, ioffset, ooffset, input, output);
	            return;
		}
	}

	public static void fast128UnPack(int width, int ioffset, int ooffset, int[] input, int[] output){
		switch (width){
			case 0:
				return;
			case 1:
				fast32UnPack1(ioffset, ooffset, input, output);
	            fast32UnPack1(ioffset + 1, ooffset + 32, input, output);
	            fast32UnPack1(ioffset + 2, ooffset + 64, input, output);
	            fast32UnPack1(ioffset + 3, ooffset + 96, input, output);
	            return;
			case 2:
				fast32UnPack2(ioffset, ooffset, input, output);
	            fast32UnPack2(ioffset + 2, ooffset + 32, input, output);
	            fast32UnPack2(ioffset + 4, ooffset + 64, input, output);
	            fast32UnPack2(ioffset + 6, ooffset + 96, input, output);
	            return;
			case 3:
				fast32UnPack3(ioffset, ooffset, input, output);
	            fast32UnPack3(ioffset + 3, ooffset + 32, input, output);
	            fast32UnPack3(ioffset + 6, ooffset + 64, input, output);
	            fast32UnPack3(ioffset + 9, ooffset + 96, input, output);
	            return;
			case 4:
				fast32UnPack4(ioffset, ooffset, input, output);
	            fast32UnPack4(ioffset + 4, ooffset + 32, input, output);
	            fast32UnPack4(ioffset + 8, ooffset + 64, input, output);
	            fast32UnPack4(ioffset + 12, ooffset + 96, input, output);
	            return;
			case 5:
				fast32UnPack5(ioffset, ooffset, input, output);
	            fast32UnPack5(ioffset + 5, ooffset + 32, input, output);
	            fast32UnPack5(ioffset + 10, ooffset + 64, input, output);
	            fast32UnPack5(ioffset + 15, ooffset + 96, input, output);
	            return;
			case 6:
				fast32UnPack6(ioffset, ooffset, input, output);
	            fast32UnPack6(ioffset + 6, ooffset + 32, input, output);
	            fast32UnPack6(ioffset + 12, ooffset + 64, input, output);
	            fast32UnPack6(ioffset + 18, ooffset + 96, input, output);
	            return;
			case 7:
				fast32UnPack7(ioffset, ooffset, input, output);
	            fast32UnPack7(ioffset + 7, ooffset + 32, input, output);
	            fast32UnPack7(ioffset + 14, ooffset + 64, input, output);
	            fast32UnPack7(ioffset + 21, ooffset + 96, input, output);
	            return;
			case 8:
				fast32UnPack8(ioffset, ooffset, input, output);
	            fast32UnPack8(ioffset + 8, ooffset + 32, input, output);
	            fast32UnPack8(ioffset + 16, ooffset + 64, input, output);
	            fast32UnPack8(ioffset + 24, ooffset + 96, input, output);
	            return;
			case 9:
				fast32UnPack9(ioffset, ooffset, input, output);
	            fast32UnPack9(ioffset + 9, ooffset + 32, input, output);
	            fast32UnPack9(ioffset + 18, ooffset + 64, input, output);
	            fast32UnPack9(ioffset + 27, ooffset + 96, input, output);
	            return;
			case 10:
				fast32UnPack10(ioffset, ooffset, input, output);
	            fast32UnPack10(ioffset + 10, ooffset + 32, input, output);
	            fast32UnPack10(ioffset + 20, ooffset + 64, input, output);
	            fast32UnPack10(ioffset + 30, ooffset + 96, input, output);
	            return;
			case 11:
				fast32UnPack11(ioffset, ooffset, input, output);
	            fast32UnPack11(ioffset + 11, ooffset + 32, input, output);
	            fast32UnPack11(ioffset + 22, ooffset + 64, input, output);
	            fast32UnPack11(ioffset + 33, ooffset + 96, input, output);
	            return;
			case 12:
				fast32UnPack12(ioffset, ooffset, input, output);
	            fast32UnPack12(ioffset + 12, ooffset + 32, input, output);
	            fast32UnPack12(ioffset + 24, ooffset + 64, input, output);
	            fast32UnPack12(ioffset + 36, ooffset + 96, input, output);
	            return;
			default:
	            PackUtils.unpack(width, 128, ioffset, ooffset, input, output);
	            return;
		}
	}	

	private static void fast32Pack(int width, int ioffset, int ooffset, int[] input , int[] output){
		switch (width){
			case 0:
	        	return;
			case 1:
				fast32Pack1(ioffset, ooffset, input, output);
				return;
			case 2:
				fast32Pack2(ioffset, ooffset, input, output);
				return;
			case 3:
				fast32Pack3(ioffset, ooffset, input, output);
				return;
			case 4:
				fast32Pack4(ioffset, ooffset, input, output);
				return;
			case 5:
				fast32Pack5(ioffset, ooffset, input, output);
				return;
			case 6:
				fast32Pack6(ioffset, ooffset, input, output);
				return;
			case 7:
				fast32Pack7(ioffset, ooffset, input, output);
				return;
			case 8:
				fast32Pack8(ioffset, ooffset, input, output);
				return;
			default:
				PackUtils.pack(width, 32, ioffset, ooffset, input, output);
				return;
		}
	}

	private static void fast32UnPack(int width, int ioffset, int ooffset, int[] input, int[] output){
		switch (width){
			case 0:
				fast32UnPack0(ioffset, ooffset, input, output);
				return;
			case 1:
				fast32UnPack1(ioffset, ooffset, input, output);
				return;
			case 2:
	        	fast32UnPack2(ioffset, ooffset, input, output);
	        	return;
			case 3:
	            fast32UnPack3(ioffset, ooffset, input, output);
	            return;
			case 4:
				fast32UnPack4(ioffset, ooffset, input, output);
				return;
			case 5:
				fast32UnPack5(ioffset, ooffset, input, output);
				return;
			case 6:
				fast32UnPack6(ioffset, ooffset, input, output);
				return;
			case 7:
				fast32UnPack7(ioffset, ooffset, input, output);
				return;
			case 8:
				fast32UnPack8(ioffset, ooffset, input, output);
				return;
			case 9:
				fast32UnPack9(ioffset, ooffset, input, output);
				return;
			case 10:
				fast32UnPack10(ioffset, ooffset, input, output);
				return;
			default:
				PackUtils.unpack(width, 32, ioffset, ooffset, input, output);
				return;
		}
	}

	private static void fast32UnPack0(int ioffset, int ooffset, int[] input, int[] output){
		for (int i = 0; i < 128; i++){
			output[ooffset+i] = 0;
		}
	}

	private static void fast32Pack1(int ioffset, int ooffset, int[] input, int[] output){
		for (int i = 0; i < 32; i++){ // will probably be unrolled
			output[ooffset] |= input[ioffset+i] << i;
		}
	}

	private static void fast32UnPack1(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = (input[ioffset]) & 1;
	    output[ooffset+1] = (input[ioffset] >>> 1) & 1;
	    output[ooffset+2] = (input[ioffset] >>> 2) & 1;
	    output[ooffset+3] = (input[ioffset] >>> 3) & 1;
	    output[ooffset+4] = (input[ioffset] >>> 4) & 1;
	    output[ooffset+5] = (input[ioffset] >>> 5) & 1;
	    output[ooffset+6] = (input[ioffset] >>> 6) & 1;
	    output[ooffset+7] = (input[ioffset] >>> 7) & 1;
	    output[ooffset+8] = (input[ioffset] >>> 8) & 1;
	    output[ooffset+9] = (input[ioffset] >>> 9) & 1;
	    output[ooffset+10] = (input[ioffset] >>> 10) & 1;
	    output[ooffset+11] = (input[ioffset] >>> 11) & 1;
	    output[ooffset+12] = (input[ioffset] >>> 12) & 1;
	    output[ooffset+13] = (input[ioffset] >>> 13) & 1;
	    output[ooffset+14] = (input[ioffset] >>> 14) & 1;
	    output[ooffset+15] = (input[ioffset] >>> 15) & 1;
	    output[ooffset+16] = (input[ioffset] >>> 16) & 1;
	    output[ooffset+17] = (input[ioffset] >>> 17) & 1;
	    output[ooffset+18] = (input[ioffset] >>> 18) & 1;
	    output[ooffset+19] = (input[ioffset] >>> 19) & 1;
	    output[ooffset+20] = (input[ioffset] >>> 20) & 1;
	    output[ooffset+21] = (input[ioffset] >>> 21) & 1;
	    output[ooffset+22] = (input[ioffset] >>> 22) & 1;
	    output[ooffset+23] = (input[ioffset] >>> 23) & 1;
	    output[ooffset+24] = (input[ioffset] >>> 24) & 1;
	    output[ooffset+25] = (input[ioffset] >>> 25) & 1;
	    output[ooffset+26] = (input[ioffset] >>> 26) & 1;
	    output[ooffset+27] = (input[ioffset] >>> 27) & 1;
	    output[ooffset+28] = (input[ioffset] >>> 28) & 1;
	    output[ooffset+29] = (input[ioffset] >>> 29) & 1;
	    output[ooffset+30] = (input[ioffset] >>> 30) & 1;
	    output[ooffset+31] = (input[ioffset] >>> 31) & 1;
	}

	private static void fast32Pack2(int ioffset, int ooffset, int[] input, int[] output){
		fast16Pack2(ioffset, ooffset, input, output);
	    fast16Pack2(ioffset + 16, ooffset + 1, input, output);
	}

	private static void fast32UnPack2(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = (input[ioffset] >>> 0) & 3;
	    output[ooffset+1] = (input[ioffset] >>> 2) & 3;
	    output[ooffset+2] = (input[ioffset] >>> 4) & 3;
	    output[ooffset+3] = (input[ioffset] >>> 6) & 3;
	    output[ooffset+4] = (input[ioffset] >>> 8) & 3;
	    output[ooffset+5] = (input[ioffset] >>> 10) & 3;
	    output[ooffset+6] = (input[ioffset] >>> 12) & 3;
	    output[ooffset+7] = (input[ioffset] >>> 14) & 3;
	    output[ooffset+8] = (input[ioffset] >>> 16) & 3;
	    output[ooffset+9] = (input[ioffset] >>> 18) & 3;
	    output[ooffset+10] = (input[ioffset] >>> 20) & 3;
	    output[ooffset+11] = (input[ioffset] >>> 22) & 3;
	    output[ooffset+12] = (input[ioffset] >>> 24) & 3;
	    output[ooffset+13] = (input[ioffset] >>> 26) & 3;
	    output[ooffset+14] = (input[ioffset] >>> 28) & 3;
	    output[ooffset+15] = (input[ioffset] >>> 30) & 3;
	    output[ooffset+16] = (input[ioffset+1] >>> 0) & 3;
	    output[ooffset+17] = (input[ioffset+1] >>> 2) & 3;
	    output[ooffset+18] = (input[ioffset+1] >>> 4) & 3;
	    output[ooffset+19] = (input[ioffset+1] >>> 6) & 3;
	    output[ooffset+20] = (input[ioffset+1] >>> 8) & 3;
	    output[ooffset+21] = (input[ioffset+1] >>> 10) & 3;
	    output[ooffset+22] = (input[ioffset+1] >>> 12) & 3;
	    output[ooffset+23] = (input[ioffset+1] >>> 14) & 3;
	    output[ooffset+24] = (input[ioffset+1] >>> 16) & 3;
	    output[ooffset+25] = (input[ioffset+1] >>> 18) & 3;
	    output[ooffset+26] = (input[ioffset+1] >>> 20) & 3;
	    output[ooffset+27] = (input[ioffset+1] >>> 22) & 3;
	    output[ooffset+28] = (input[ioffset+1] >>> 24) & 3;
	    output[ooffset+29] = (input[ioffset+1] >>> 26) & 3;
	    output[ooffset+30] = (input[ioffset+1] >>> 28) & 3;
	    output[ooffset+31] = (input[ioffset+1] >>> 30) & 3;
	}

	private static void fast16Pack2(int ioffset, int ooffset, int[] input, int[] output){
		for (int i = 0; i < 16; i++){ // will probably be unrolled
			output[ooffset] |= input[ioffset+i] << (i << 1);
		}
	}

	private static void fast32Pack3(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 3;
	    output[ooffset] |= input[ioffset+2] << 6;
	    output[ooffset] |= input[ioffset+3] << 9;
	    output[ooffset] |= input[ioffset+4] << 12;
	    output[ooffset] |= input[ioffset+5] << 15;
	    output[ooffset] |= input[ioffset+6] << 18;
	    output[ooffset] |= input[ioffset+7] << 21;
	    output[ooffset] |= input[ioffset+8] << 24;
	    output[ooffset] |= input[ioffset+9] << 27;
	    output[ooffset] |= input[ioffset+10] << 30;
	    output[ooffset+1] = input[ioffset+10] >>> 2;
	    
	    output[ooffset+1] |= input[ioffset+11] << 1;
	    output[ooffset+1] |= input[ioffset+12] << 4;
	    output[ooffset+1] |= input[ioffset+13] << 7;
	    output[ooffset+1] |= input[ioffset+14] << 10;
	    output[ooffset+1] |= input[ioffset+15] << 13;
	    output[ooffset+1] |= input[ioffset+16] << 16;
	    output[ooffset+1] |= input[ioffset+17] << 19;
	    output[ooffset+1] |= input[ioffset+18] << 22;
	    output[ooffset+1] |= input[ioffset+19] << 25;
	    output[ooffset+1] |= input[ioffset+20] << 28;
	    output[ooffset+1] |= input[ioffset+21] << 31;
	    output[ooffset+2] = input[ioffset+21] >>> 1;

	    output[ooffset+2] |= input[ioffset+22] << 2;
	    output[ooffset+2] |= input[ioffset+23] << 5;
	    output[ooffset+2] |= input[ioffset+24] << 8;
	    output[ooffset+2] |= input[ioffset+25] << 11;
	    output[ooffset+2] |= input[ioffset+26] << 14;
	    output[ooffset+2] |= input[ioffset+27] << 17;
	    output[ooffset+2] |= input[ioffset+28] << 20;
	    output[ooffset+2] |= input[ioffset+29] << 23;
	    output[ooffset+2] |= input[ioffset+30] << 26;
	    output[ooffset+2] |= input[ioffset+31] << 29;
	}

	private static void fast32UnPack3(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = 7 & input[ioffset];
	    output[ooffset+1] = 7 & (input[ioffset] >>> 3);
	    output[ooffset+2] = 7 & (input[ioffset] >>> 6);
	    output[ooffset+3] = 7 & (input[ioffset] >>> 9);
	    output[ooffset+4] = 7 & (input[ioffset] >>> 12);
	    output[ooffset+5] = 7 & (input[ioffset] >>> 15);
	    output[ooffset+6] = 7 & (input[ioffset] >>> 18);
	    output[ooffset+7] = 7 & (input[ioffset] >>> 21);
	    output[ooffset+8] = 7 & (input[ioffset] >>> 24);
	    output[ooffset+9] = 7 & (input[ioffset] >>> 27);
	    output[ooffset+10] = 3 & (input[ioffset] >>> 30);
	    output[ooffset+10] |= 4 & (input[ioffset+1] << 2);

        output[ooffset+11] = 7 & (input[ioffset+1] >>> 1);
        output[ooffset+12] = 7 & (input[ioffset+1] >>> 4);
        output[ooffset+13] = 7 & (input[ioffset+1] >>> 7);
        output[ooffset+14] = 7 & (input[ioffset+1] >>> 10);
        output[ooffset+15] = 7 & (input[ioffset+1] >>> 13);
        output[ooffset+16] = 7 & (input[ioffset+1] >>> 16);
        output[ooffset+17] = 7 & (input[ioffset+1] >>> 19);
        output[ooffset+18] = 7 & (input[ioffset+1] >>> 22);
        output[ooffset+19] = 7 & (input[ioffset+1] >>> 25);
        output[ooffset+20] = 7 & (input[ioffset+1] >>> 28);
        output[ooffset+21] = 1 & (input[ioffset+1] >>> 31);
        output[ooffset+21] |= 6 & (input[ioffset+2] << 1);
        output[ooffset+22] = 7 & (input[ioffset+2] >>> 2);
        output[ooffset+23] = 7 & (input[ioffset+2] >>> 5);
        output[ooffset+24] = 7 & (input[ioffset+2] >>> 8);
        output[ooffset+25] = 7 & (input[ioffset+2] >>> 11);
        output[ooffset+26] = 7 & (input[ioffset+2] >>> 14);
        output[ooffset+27] = 7 & (input[ioffset+2] >>> 17);
        output[ooffset+28] = 7 & (input[ioffset+2] >>> 20);
        output[ooffset+29] = 7 & (input[ioffset+2] >>> 23);
        output[ooffset+30] = 7 & (input[ioffset+2] >>> 26);
        output[ooffset+31] = 7 & (input[ioffset+2] >>> 29);
	}

	private static void fast8Pack4(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 4;
	    output[ooffset] |= input[ioffset+2] << 8;
	    output[ooffset] |= input[ioffset+3] << 12;
	    output[ooffset] |= input[ioffset+4] << 16;
	    output[ooffset] |= input[ioffset+5] << 20;
	    output[ooffset] |= input[ioffset+6] << 24;
	    output[ooffset] |= input[ioffset+7] << 28;
	}

	private static void fast32Pack4(int ioffset, int ooffset, int[] input, int[] output){
		fast8Pack4(ioffset, ooffset, input, output);
		fast8Pack4(ioffset + 8, ooffset + 1, input, output);
		fast8Pack4(ioffset + 16, ooffset + 2, input, output);
		fast8Pack4(ioffset + 24, ooffset + 3, input, output);
	}

	private static void fast32UnPack4(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = (input[ioffset] >>> 0) & 15;
	    output[ooffset+1] = (input[ioffset] >>> 4) & 15;
	    output[ooffset+2] = (input[ioffset] >>> 8) & 15;
	    output[ooffset+3] = (input[ioffset] >>> 12) & 15;
	    output[ooffset+4] = (input[ioffset] >>> 16) & 15;
	    output[ooffset+5] = (input[ioffset] >>> 20) & 15;
	    output[ooffset+6] = (input[ioffset] >>> 24) & 15;
	    output[ooffset+7] = (input[ioffset] >>> 28) & 15;
	    output[ooffset+8] = (input[ioffset+1] >>> 0) & 15;
	    output[ooffset+9] = (input[ioffset+1] >>> 4) & 15;
	    output[ooffset+10] = (input[ioffset+1] >>> 8) & 15;
	    output[ooffset+11] = (input[ioffset+1] >>> 12) & 15;
	    output[ooffset+12] = (input[ioffset+1] >>> 16) & 15;
	    output[ooffset+13] = (input[ioffset+1] >>> 20) & 15;
	    output[ooffset+14] = (input[ioffset+1] >>> 24) & 15;
	    output[ooffset+15] = (input[ioffset+1] >>> 28) & 15;
	    output[ooffset+16] = (input[ioffset+2] >>> 0) & 15;
	    output[ooffset+17] = (input[ioffset+2] >>> 4) & 15;
	    output[ooffset+18] = (input[ioffset+2] >>> 8) & 15;
	    output[ooffset+19] = (input[ioffset+2] >>> 12) & 15;
	    output[ooffset+20] = (input[ioffset+2] >>> 16) & 15;
	    output[ooffset+21] = (input[ioffset+2] >>> 20) & 15;
	    output[ooffset+22] = (input[ioffset+2] >>> 24) & 15;
	    output[ooffset+23] = (input[ioffset+2] >>> 28) & 15;
	    output[ooffset+24] = (input[ioffset+3] >>> 0) & 15;
	    output[ooffset+25] = (input[ioffset+3] >>> 4) & 15;
	    output[ooffset+26] = (input[ioffset+3] >>> 8) & 15;
	    output[ooffset+27] = (input[ioffset+3] >>> 12) & 15;
	    output[ooffset+28] = (input[ioffset+3] >>> 16) & 15;
	    output[ooffset+29] = (input[ioffset+3] >>> 20) & 15;
	    output[ooffset+30] = (input[ioffset+3] >>> 24) & 15;
	    output[ooffset+31] = (input[ioffset+3] >>> 28) & 15;
	}

    private static void fast32Pack5(int ioffset, int ooffset, int[] input, int[] output){
    	output[ooffset] |= 31 & input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 5;
	    output[ooffset] |= input[ioffset+2] << 10;
	    output[ooffset] |= input[ioffset+3] << 15;
	    output[ooffset] |= input[ioffset+4] << 20;
	    output[ooffset] |= input[ioffset+5] << 25;
	    output[ooffset] |= input[ioffset+6] << 30;
	    output[ooffset+1] |= input[ioffset+6] >>> 2;

	    output[ooffset+1] |= input[ioffset+7] << 3;
	    output[ooffset+1] |= input[ioffset+8] << 8;
	    output[ooffset+1] |= input[ioffset+9] << 13;
	    output[ooffset+1] |= input[ioffset+10] << 18;
	    output[ooffset+1] |= input[ioffset+11] << 23;
	    output[ooffset+1] |= input[ioffset+12] << 28;
	    output[ooffset+2] |= input[ioffset+12] >>> 4;

	    output[ooffset+2] |= input[ioffset+13] << 1;
	    output[ooffset+2] |= input[ioffset+14] << 6;
	    output[ooffset+2] |= input[ioffset+15] << 11;
	    output[ooffset+2] |= input[ioffset+16] << 16;
	    output[ooffset+2] |= input[ioffset+17] << 21;
	    output[ooffset+2] |= input[ioffset+18] << 26;
	    output[ooffset+2] |= input[ioffset+19] << 31;
	    output[ooffset+3] |= input[ioffset+19] >>> 1;

	    output[ooffset+3] |= input[ioffset+20] << 4;
	    output[ooffset+3] |= input[ioffset+21] << 9;
	    output[ooffset+3] |= input[ioffset+22] << 14;
	    output[ooffset+3] |= input[ioffset+23] << 19;
	    output[ooffset+3] |= input[ioffset+24] << 24;
	    output[ooffset+3] |= input[ioffset+25] << 29;
	    output[ooffset+4] |= input[ioffset+25] >>> 3;

	    output[ooffset+4] |= input[ioffset+26] << 2;
	    output[ooffset+4] |= input[ioffset+27] << 7;
	    output[ooffset+4] |= input[ioffset+28] << 12;
	    output[ooffset+4] |= input[ioffset+29] << 17;
	    output[ooffset+4] |= input[ioffset+30] << 22;
	    output[ooffset+4] |= input[ioffset+31] << 27;
	}

	private static void fast32UnPack5(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = 31 & input[ioffset];
	    output[ooffset+1] = 31 & (input[ioffset] >>> 5);
	    output[ooffset+2] = 31 & (input[ioffset] >>> 10);
	    output[ooffset+3] = 31 & (input[ioffset] >>> 15);
	    output[ooffset+4] = 31 & (input[ioffset] >>> 20);
	    output[ooffset+5] = 31 & (input[ioffset] >>> 25);
	    output[ooffset+6] = 3 & (input[ioffset] >>> 30);
	    output[ooffset+6] |= 28 & (input[ioffset+1] << 2);

	    output[ooffset+7] = 31 & (input[ioffset+1] >>> 3);
	    output[ooffset+8] = 31 & (input[ioffset+1] >>> 8);
	    output[ooffset+9] = 31 & (input[ioffset+1] >>> 13);
	    output[ooffset+10] = 31 & (input[ioffset+1] >>> 18);
	    output[ooffset+11] = 31 & (input[ioffset+1] >>> 23);
	    output[ooffset+12] = 15 & (input[ioffset+1] >>> 28);
	    output[ooffset+12] |= 16 & (input[ioffset+2] << 4);

        output[ooffset+13] = 31 & (input[ioffset+2] >>> 1);
        output[ooffset+14] = 31 & (input[ioffset+2] >>> 6);
        output[ooffset+15] = 31 & (input[ioffset+2] >>> 11);
        output[ooffset+16] = 31 & (input[ioffset+2] >>> 16);
        output[ooffset+17] = 31 & (input[ioffset+2] >>> 21);
        output[ooffset+18] = 31 & (input[ioffset+2] >>> 26);
        output[ooffset+19] = 1 & (input[ioffset+2] >>> 31);
        output[ooffset+19] |= 30 & (input[ioffset+3] << 1);

        output[ooffset+20] = 31 & (input[ioffset+3] >>> 4);
	    output[ooffset+21] = 31 & (input[ioffset+3] >>> 9);
	    output[ooffset+22] = 31 & (input[ioffset+3] >>> 14);
	    output[ooffset+23] = 31 & (input[ioffset+3] >>> 19);
	    output[ooffset+24] = 31 & (input[ioffset+3] >>> 24);
	    output[ooffset+25] = 7 & (input[ioffset+3] >>> 29);
	    output[ooffset+25] |= 24 & (input[ioffset+4] << 3);

	    output[ooffset+26] = 31 & (input[ioffset+4] >>> 2);
	    output[ooffset+27] = 31 & (input[ioffset+4] >>> 7);
	    output[ooffset+28] = 31 & (input[ioffset+4] >>> 12);
	    output[ooffset+29] = 31 & (input[ioffset+4] >>> 17);
	    output[ooffset+30] = 31 & (input[ioffset+4] >>> 22);
	    output[ooffset+31] = 31 & (input[ioffset+4] >>> 27);
	}

	private static void fast32Pack6(int ioffset, int ooffset, int[] input, int[] output){
		fast16Pack6(ioffset, ooffset, input, output);
		fast16Pack6(ioffset + 16, ooffset + 3, input, output);
	}

    private static void fast32UnPack6(int ioffset, int ooffset, int[] input, int[] output){
    	output[ooffset] = (input[ioffset] >>> 0) & 63;
	    output[ooffset+1] = (input[ioffset] >>> 6) & 63;
	    output[ooffset+2] = (input[ioffset] >>> 12) & 63;
	    output[ooffset+3] = (input[ioffset] >>> 18) & 63;
	    output[ooffset+4] = (input[ioffset] >>> 24) & 63;
	    output[ooffset+5] = (input[ioffset] >>> 30) & 3;
	    output[ooffset+5] |= (input[ioffset+1] << 2) & 60;

	    output[ooffset+6] = (input[ioffset+1] >>> 4) & 63;
	    output[ooffset+7] = (input[ioffset+1] >>> 10) & 63;
	    output[ooffset+8] = (input[ioffset+1] >>> 16) & 63;
	    output[ooffset+9] = (input[ioffset+1] >>> 22) & 63;
	    output[ooffset+10] = (input[ioffset+1] >>> 28) & 15;
	    output[ooffset+10] |= (input[ioffset+2] << 4) & 48;

	    output[ooffset+11] = (input[ioffset+2] >>> 2) & 63;
	    output[ooffset+12] = (input[ioffset+2] >>> 8) & 63;
	    output[ooffset+13] = (input[ioffset+2] >>> 14) & 63;
	    output[ooffset+14] = (input[ioffset+2] >>> 20) & 63;
	    output[ooffset+15] = (input[ioffset+2] >>> 26) & 63;
	    output[ooffset+16] = (input[ioffset+3] >>> 0) & 63;
	    output[ooffset+17] = (input[ioffset+3] >>> 6) & 63;
	    output[ooffset+18] = (input[ioffset+3] >>> 12) & 63;
	    output[ooffset+19] = (input[ioffset+3] >>> 18) & 63;
	    output[ooffset+20] = (input[ioffset+3] >>> 24) & 63;
	    output[ooffset+21] = (input[ioffset+3] >>> 30) & 3;
	    output[ooffset+21] |= (input[ioffset+4] << 2) & 60;

	    output[ooffset+22] = (input[ioffset+4] >>> 4) & 63;
	    output[ooffset+23] = (input[ioffset+4] >>> 10) & 63;
	    output[ooffset+24] = (input[ioffset+4] >>> 16) & 63;
	    output[ooffset+25] = (input[ioffset+4] >>> 22) & 63;
	    output[ooffset+26] = (input[ioffset+4] >>> 28) & 15;
	    output[ooffset+26] |= (input[ioffset+5] << 4) & 48;

	    output[ooffset+27] = (input[ioffset+5] >>> 2) & 63;
	    output[ooffset+28] = (input[ioffset+5] >>> 8) & 63;
	    output[ooffset+29] = (input[ioffset+5] >>> 14) & 63;
	    output[ooffset+30] = (input[ioffset+5] >>> 20) & 63;
	    output[ooffset+31] = (input[ioffset+5] >>> 26) & 63;
	}

	private static void fast16Pack6(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] |= input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 6;
	    output[ooffset] |= input[ioffset+2] << 12;
	    output[ooffset] |= input[ioffset+3] << 18;
	    output[ooffset] |= input[ioffset+4] << 24;
	    output[ooffset] |= input[ioffset+5] << 30;
	    output[ooffset+1] |= input[ioffset+5] >>> 2;

	    output[ooffset+1] |= input[ioffset+6] << 4;
	    output[ooffset+1] |= input[ioffset+7] << 10;
	    output[ooffset+1] |= input[ioffset+8] << 16;
	    output[ooffset+1] |= input[ioffset+9] << 22;
	    output[ooffset+1] |= input[ioffset+10] << 28;
	    output[ooffset+2] |= input[ioffset+10] >>> 4;

	    output[ooffset+2] |= input[ioffset+11] << 2;
	    output[ooffset+2] |= input[ioffset+12] << 8;
	    output[ooffset+2] |= input[ioffset+13] << 14;
	    output[ooffset+2] |= input[ioffset+14] << 20;
	    output[ooffset+2] |= input[ioffset+15] << 26;
	}

	private static void fast32Pack7(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 7;
	    output[ooffset] |= input[ioffset+2] << 14;
	    output[ooffset] |= input[ioffset+3] << 21;
	    output[ooffset] |= input[ioffset+4] << 28;
	    output[ooffset+1] = input[ioffset+4] >>> 4;

        output[ooffset+1] |= input[ioffset+5] << 3;
        output[ooffset+1] |= input[ioffset+6] << 10;
        output[ooffset+1] |= input[ioffset+7] << 17;
        output[ooffset+1] |= input[ioffset+8] << 24;
        output[ooffset+1] |= input[ioffset+9] << 31;
        output[ooffset+2] = input[ioffset+9] >>> 1;
        output[ooffset+2] |= input[ioffset+10] << 6;
        output[ooffset+2] |= input[ioffset+11] << 13;
        output[ooffset+2] |= input[ioffset+12] << 20;
        output[ooffset+2] |= input[ioffset+13] << 27;
        output[ooffset+3] = input[ioffset+13] >>> 5;
	                
        output[ooffset+3] |= input[ioffset+14] << 2;
        output[ooffset+3] |= input[ioffset+15] << 9;
        output[ooffset+3] |= input[ioffset+16] << 16;
        output[ooffset+3] |= input[ioffset+17] << 23;
        output[ooffset+3] |= input[ioffset+18] << 30;
        output[ooffset+4] = input[ioffset+18] >>> 2;

        output[ooffset+4] |= input[ioffset+19] << 5;
	    output[ooffset+4] |= input[ioffset+20] << 12;
	    output[ooffset+4] |= input[ioffset+21] << 19;
	    output[ooffset+4] |= input[ioffset+22] << 26;
	    output[ooffset+5] = input[ioffset+22] >>> 6;
	                
	    output[ooffset+5] |= input[ioffset+23] << 1;
	    output[ooffset+5] |= input[ioffset+24] << 8;
	    output[ooffset+5] |= input[ioffset+25] << 15;
	    output[ooffset+5] |= input[ioffset+26] << 22;
	    output[ooffset+5] |= input[ioffset+27] << 29;
	    output[ooffset+6] = input[ioffset+27] >>> 3;

	    output[ooffset+6] |= input[ioffset+28] << 4;
	    output[ooffset+6] |= input[ioffset+29] << 11;
	    output[ooffset+6] |= input[ioffset+30] << 18;
	    output[ooffset+6] |= input[ioffset+31] << 25;
	}

	private static void fast32UnPack7(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = 127 & input[ioffset];
	    output[ooffset+1] = 127 & (input[ioffset] >>> 7);
	    output[ooffset+2] = 127 & (input[ioffset] >>> 14);
	    output[ooffset+3] = 127 & (input[ioffset] >>> 21);
	    output[ooffset+4] = 15 & (input[ioffset] >>> 28);
	    output[ooffset+4] |= 112 & (input[ioffset+1] << 4);

	    output[ooffset+5] = 127 & (input[ioffset+1] >>> 3);
	    output[ooffset+6] = 127 & (input[ioffset+1] >>> 10);
	    output[ooffset+7] = 127 & (input[ioffset+1] >>> 17);
	    output[ooffset+8] = 127 & (input[ioffset+1] >>> 24);
	    output[ooffset+9] = 1 & (input[ioffset+1] >>> 31);
	    output[ooffset+9] |= 126 & (input[ioffset+2] << 1);

	    output[ooffset+10] = 127 & (input[ioffset+2] >>> 6);
	    output[ooffset+11] = 127 & (input[ioffset+2] >>> 13);
	    output[ooffset+12] = 127 & (input[ioffset+2] >>> 20);
	    output[ooffset+13] = 31 & (input[ioffset+2] >>> 27);
	    output[ooffset+13] |= 96 & (input[ioffset+3] << 5);

	    output[ooffset+14] = 127 & (input[ioffset+3] >>> 2);
	    output[ooffset+15] = 127 & (input[ioffset+3] >>> 9);
	    output[ooffset+16] = 127 & (input[ioffset+3] >>> 16);
	    output[ooffset+17] = 127 & (input[ioffset+3] >>> 23);
	    output[ooffset+18] = 3 & (input[ioffset+3] >>> 30);
	    output[ooffset+18] |= 124 & (input[ioffset+4] << 2);

	    output[ooffset+19] = 127 & (input[ioffset+4] >>> 5);
	    output[ooffset+20] = 127 & (input[ioffset+4] >>> 12);
	    output[ooffset+21] = 127 & (input[ioffset+4] >>> 19);
	    output[ooffset+22] = 63 & (input[ioffset+4] >>> 26);
	    output[ooffset+22] |= 64 & (input[ioffset+5] << 6);

	    output[ooffset+23] = 127 & (input[ioffset+5] >>> 1);
	    output[ooffset+24] = 127 & (input[ioffset+5] >>> 8);
	    output[ooffset+25] = 127 & (input[ioffset+5] >>> 15);
	    output[ooffset+26] = 127 & (input[ioffset+5] >>> 22);
	    output[ooffset+27] = 7 & (input[ioffset+5] >>> 29);
	    output[ooffset+27] |= 120 & (input[ioffset+6] << 3);

	    output[ooffset+28] = 127 & (input[ioffset+6] >>> 4);
	    output[ooffset+29] = 127 & (input[ioffset+6] >>> 11);
	    output[ooffset+30] = 127 & (input[ioffset+6] >>> 18);
	    output[ooffset+31] = 127 & (input[ioffset+6] >>> 25);
	}

	private static void fast32Pack8(int ioffset, int ooffset, int[] input, int[] output){
		fast4Pack8(ioffset, ooffset, input, output);
		fast4Pack8(ioffset + 4, ooffset + 1, input, output);
		fast4Pack8(ioffset + 8, ooffset + 2, input, output);
		fast4Pack8(ioffset + 12, ooffset + 3, input, output);
		fast4Pack8(ioffset + 16, ooffset + 4, input, output);
		fast4Pack8(ioffset + 20, ooffset + 5, input, output);
		fast4Pack8(ioffset + 24, ooffset + 6, input, output);
		fast4Pack8(ioffset + 28, ooffset + 7, input, output);
	}

	private static void fast32UnPack8(int ioffset, int ooffset, int[] input, int[] output){
		fast4UnPack8(ioffset, ooffset, input, output);
	    fast4UnPack8(ioffset + 1, ooffset + 4, input, output);
	    fast4UnPack8(ioffset + 2, ooffset + 8, input, output);
	    fast4UnPack8(ioffset + 3, ooffset + 12, input, output);
	    fast4UnPack8(ioffset + 4, ooffset + 16, input, output);
	    fast4UnPack8(ioffset + 5, ooffset + 20, input, output);
	    fast4UnPack8(ioffset + 6, ooffset + 24, input, output);
	    fast4UnPack8(ioffset + 7, ooffset + 28, input, output);
	}

	private static void fast4Pack8(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] |= input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 8;
	    output[ooffset] |= input[ioffset+2] << 16;
	    output[ooffset] |= input[ioffset+3] << 24;
	}

	private static void fast4UnPack8(int ioffset, int ooffset, int[] input, int[] output){
	    output[ooffset] = 255 & input[ioffset];
	    output[ooffset+1] = 255 & (input[ioffset] >>> 8);
	    output[ooffset+2] = 255 & (input[ioffset] >>> 16);
	    output[ooffset+3] = 255 & (input[ioffset] >>> 24);
	}

	private static void fast32Pack9(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset];
	    output[ooffset] |= input[ioffset+1] << 9;
	    output[ooffset] |= input[ioffset+2] << 18;
	    output[ooffset] |= input[ioffset+3] << 27;
	    output[ooffset+1] = input[ioffset+3] >>> 5;

	    output[ooffset+1] |= input[ioffset+4] << 4;
	    output[ooffset+1] |= input[ioffset+5] << 13;
	    output[ooffset+1] |= input[ioffset+6] << 22;
	    output[ooffset+1] |= input[ioffset+7] << 31;
        output[ooffset+2] = input[ioffset+7] >>> 1;

        output[ooffset+2] |= input[ioffset+8] << 8;
        output[ooffset+2] |= input[ioffset+9] << 17;
        output[ooffset+2] |= input[ioffset+10] << 26;
        output[ooffset+3] = input[ioffset+10] >>> 6;

        output[ooffset+3] |= input[ioffset+11] << 3;
	    output[ooffset+3] |= input[ioffset+12] << 12;
	    output[ooffset+3] |= input[ioffset+13] << 21;
	    output[ooffset+3] |= input[ioffset+14] << 30;
	    output[ooffset+4] = input[ioffset+14] >>> 2;

	    output[ooffset+4] |= input[ioffset+15] << 7;
	    output[ooffset+4] |= input[ioffset+16] << 16;
	    output[ooffset+4] |= input[ioffset+17] << 25;
	    output[ooffset+5] = input[ioffset+17] >>> 7;

	    output[ooffset+5] |= input[ioffset+18] << 2;
	    output[ooffset+5] |= input[ioffset+19] << 11;
	    output[ooffset+5] |= input[ioffset+20] << 20;
	    output[ooffset+5] |= input[ioffset+21] << 29;
	    output[ooffset+6] = input[ioffset+21] >>> 3;

	    output[ooffset+6] |= input[ioffset+22] << 6;
	    output[ooffset+6] |= input[ioffset+23] << 15;
	    output[ooffset+6] |= input[ioffset+24] << 24;
	    output[ooffset+7] = input[ioffset+24] >>> 8;

	    output[ooffset+7] |= input[ioffset+25] << 1;
	    output[ooffset+7] |= input[ioffset+26] << 10;
	    output[ooffset+7] |= input[ioffset+27] << 19;
	    output[ooffset+7] |= input[ioffset+28] << 28;
	    output[ooffset+8] = input[ioffset+28] >>> 4;

	    output[ooffset+8] |= input[ioffset+29] << 5;
	    output[ooffset+8] |= input[ioffset+30] << 14;
	    output[ooffset+8] |= input[ioffset+31] << 23;
	}

	private static void fast32UnPack9(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = (input[ioffset] >>> 0) & 511;
	    output[ooffset+1] = (input[ioffset] >>> 9) & 511;
	    output[ooffset+2] = (input[ioffset] >>> 18) & 511;
	    output[ooffset+3] = (input[ioffset] >>> 27) & 31;
	    output[ooffset+3] |= (input[ioffset+1] << 5) & 480;

	    output[ooffset+4] = (input[ioffset+1] >>> 4) & 511;
	    output[ooffset+5] = (input[ioffset+1] >>> 13) & 511;
	    output[ooffset+6] = (input[ioffset+1] >>> 22) & 511;
	    output[ooffset+7] = (input[ioffset+1] >>> 31) & 1;
	    output[ooffset+7] |= (input[ioffset+2] << 1) & 510;

	    output[ooffset+8] = (input[ioffset+2] >>> 8) & 511;
	    output[ooffset+9] = (input[ioffset+2] >>> 17) & 511;
	    output[ooffset+10] = (input[ioffset+2] >>> 26) & 63;
	    output[ooffset+10] |= (input[ioffset+3] << 6) & 448;

	    output[ooffset+11] = (input[ioffset+3] >>> 3) & 511;
	    output[ooffset+12] = (input[ioffset+3] >>> 12) & 511;
	    output[ooffset+13] = (input[ioffset+3] >>> 21) & 511;
	    output[ooffset+14] = (input[ioffset+3] >>> 30) & 3;
	    output[ooffset+14] |= (input[ioffset+4] << 2) & 508;

	    output[ooffset+15] = (input[ioffset+4] >>> 7) & 511;
	    output[ooffset+16] = (input[ioffset+4] >>> 16) & 511;
	    output[ooffset+17] = (input[ioffset+4] >>> 25) & 127;
	    output[ooffset+17] |= (input[ioffset+5] << 7) & 384;

	    output[ooffset+18] = (input[ioffset+5] >>> 2) & 511;
	    output[ooffset+19] = (input[ioffset+5] >>> 11) & 511;
	    output[ooffset+20] = (input[ioffset+5] >>> 20) & 511;
	    output[ooffset+21] = (input[ioffset+5] >>> 29) & 7;
	    output[ooffset+21] |= (input[ioffset+6] << 3) & 504;

	    output[ooffset+22] = (input[ioffset+6] >>> 6) & 511;
	    output[ooffset+23] = (input[ioffset+6] >>> 15) & 511;
	    output[ooffset+24] = (input[ioffset+6] >>> 24) & 255;
	    output[ooffset+24] |= (input[ioffset+7] << 8) & 256;

	    output[ooffset+25] = (input[ioffset+7] >>> 1) & 511;
	    output[ooffset+26] = (input[ioffset+7] >>> 10) & 511;
	    output[ooffset+27] = (input[ioffset+7] >>> 19) & 511;
	    output[ooffset+28] = (input[ioffset+7] >>> 28) & 15;
	    output[ooffset+28] |= (input[ioffset+8] << 4) & 496;

	    output[ooffset+29] = (input[ioffset+8] >>> 5) & 511;
	    output[ooffset+30] = (input[ioffset+8] >>> 14) & 511;
	    output[ooffset+31] = (input[ioffset+8] >>> 23) & 511;
	}

	private static void fast32UnPack10(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = (input[ioffset] >>> 0) & 1023;
	    output[ooffset+1] = (input[ioffset] >>> 10) & 1023;
	    output[ooffset+2] = (input[ioffset] >>> 20) & 1023;
	    output[ooffset+3] = (input[ioffset] >>> 30) & 3;
	    output[ooffset+3] |= (input[ioffset+1] << 2) & 1020;

	    output[ooffset+4] = (input[ioffset+1] >>> 8) & 1023;
	    output[ooffset+5] = (input[ioffset+1] >>> 18) & 1023;
	    output[ooffset+6] = (input[ioffset+1] >>> 28) & 15;
	    output[ooffset+6] |= (input[ioffset+2] << 4) & 1008;

	    output[ooffset+7] = (input[ioffset+2] >>> 6) & 1023;
	    output[ooffset+8] = (input[ioffset+2] >>> 16) & 1023;
	    output[ooffset+9] = (input[ioffset+2] >>> 26) & 63;
	    output[ooffset+9] |= (input[ioffset+3] << 6) & 960;

        output[ooffset+10] = (input[ioffset+3] >>> 4) & 1023;
        output[ooffset+11] = (input[ioffset+3] >>> 14) & 1023;
        output[ooffset+12] = (input[ioffset+3] >>> 24) & 255;
        output[ooffset+12] |= (input[ioffset+4] << 8) & 768;

        output[ooffset+13] = (input[ioffset+4] >>> 2) & 1023;
        output[ooffset+14] = (input[ioffset+4] >>> 12) & 1023;
        output[ooffset+15] = (input[ioffset+4] >>> 22) & 1023;
        output[ooffset+16] = (input[ioffset+5] >>> 0) & 1023;
        output[ooffset+17] = (input[ioffset+5] >>> 10) & 1023;
        output[ooffset+18] = (input[ioffset+5] >>> 20) & 1023;
        output[ooffset+19] = (input[ioffset+5] >>> 30) & 3;
        output[ooffset+19] |= (input[ioffset+6] << 2) & 1020;

        output[ooffset+20] = (input[ioffset+6] >>> 8) & 1023;
        output[ooffset+21] = (input[ioffset+6] >>> 18) & 1023;
        output[ooffset+22] = (input[ioffset+6] >>> 28) & 15;
        output[ooffset+22] |= (input[ioffset+7] << 4) & 1008;

        output[ooffset+23] = (input[ioffset+7] >>> 6) & 1023;
	    output[ooffset+24] = (input[ioffset+7] >>> 16) & 1023;
	    output[ooffset+25] = (input[ioffset+7] >>> 26) & 63;
	    output[ooffset+25] |= (input[ioffset+8] << 6) & 960;

	    output[ooffset+26] = (input[ioffset+8] >>> 4) & 1023;
	    output[ooffset+27] = (input[ioffset+8] >>> 14) & 1023;
	    output[ooffset+28] = (input[ioffset+8] >>> 24) & 255;
        output[ooffset+28] |= (input[ioffset+9] << 8) & 768;

        output[ooffset+29] = (input[ioffset+9] >>> 2) & 1023;
        output[ooffset+30] = (input[ioffset+9] >>> 12) & 1023;
        output[ooffset+31] = (input[ioffset+9] >>> 22) & 1023;
	}

	private static void fast32UnPack11(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset] & 2047;
		output[ooffset+1] = (input[ioffset] >>> 11) & 2047;
		output[ooffset+2] = (input[ioffset] >>> 22) & 1023;
	    output[ooffset+2] |= (input[ioffset+1] << 10) & 1024;

	    output[ooffset+3] = (input[ioffset+1] >>> 1) & 2047;
	    output[ooffset+4] = (input[ioffset+1] >>> 12) & 2047;
	    output[ooffset+5] = (input[ioffset+1] >>> 23) & 511;
	    output[ooffset+5] |= (input[ioffset+2] << 9) & 1536;

	    output[ooffset+6] = (input[ioffset+2] >>> 2) & 2047;
	    output[ooffset+7] = (input[ioffset+2] >>> 13) & 2047;
	    output[ooffset+8] = (input[ioffset+2] >>> 24) & 255;
	    output[ooffset+8] |= (input[ioffset+3] << 8) & 1792;

	    output[ooffset+9] = (input[ioffset+3] >>> 3) & 2047;
	    output[ooffset+10] = (input[ioffset+3] >>> 14) & 2047;
	    output[ooffset+11] = (input[ioffset+3] >>> 25) & 127;
	    output[ooffset+11] |= (input[ioffset+4] << 7) & 1920;

	    output[ooffset+12] = (input[ioffset+4] >>> 4) & 2047;
	    output[ooffset+13] = (input[ioffset+4] >>> 15) & 2047;
	    output[ooffset+14] = (input[ioffset+4] >>> 26) & 63;
	    output[ooffset+14] |= (input[ioffset+5] << 6) & 1984;

	    output[ooffset+15] = (input[ioffset+5] >>> 5) & 2047;
	    output[ooffset+16] = (input[ioffset+5] >>> 16) & 2047;
	    output[ooffset+17] = (input[ioffset+5] >>> 27) & 31;
	    output[ooffset+17] |= (input[ioffset+6] << 5) & 2016;

	    output[ooffset+18] = (input[ioffset+6] >>> 6) & 2047;
	    output[ooffset+19] = (input[ioffset+6] >>> 17) & 2047;
	    output[ooffset+20] = (input[ioffset+6] >>> 28) & 15;
	    output[ooffset+20] |= (input[ioffset+7] << 4) & 2032;

	    output[ooffset+21] = (input[ioffset+7] >>> 7) & 2047;
	    output[ooffset+22] = (input[ioffset+7] >>> 18) & 2047;
	    output[ooffset+23] = (input[ioffset+7] >>> 29) & 7;
	    output[ooffset+23] |= (input[ioffset+8] << 3) & 2040;

	    output[ooffset+24] = (input[ioffset+8] >>> 8) & 2047;
	    output[ooffset+25] = (input[ioffset+8] >>> 19) & 2047;
	    output[ooffset+26] = (input[ioffset+8] >>> 30) & 3;
	    output[ooffset+26] |= (input[ioffset+9] << 2) & 2044;

	    output[ooffset+27] = (input[ioffset+9] >>> 9) & 2047;
	    output[ooffset+28] = (input[ioffset+9] >>> 20) & 2047;
	    output[ooffset+29] = (input[ioffset+9] >>> 31) & 1;
	    output[ooffset+29] |= (input[ioffset+10] << 1) & 2046;

	    output[ooffset+30] = (input[ioffset+10] >>> 10) & 2047;
	    output[ooffset+31] = (input[ioffset+10] >>> 21) & 2047;
	}

	private static void fast32UnPack12(int ioffset, int ooffset, int[] input, int[] output){
		output[ooffset] = input[ioffset] & 4095;
	    output[ooffset+1] = (input[ioffset] >>> 12) & 4095;
	    output[ooffset+2] = (input[ioffset] >>> 24) & 255;
	    output[ooffset+2] |= (input[ioffset+1] << 8) & 3840;

	    output[ooffset+3] = (input[ioffset+1] >>> 4) & 4095;
	    output[ooffset+4] = (input[ioffset+1] >>> 16) & 4095;
	    output[ooffset+5] = (input[ioffset+1] >>> 28) & 15;
	    output[ooffset+5] |= (input[ioffset+2] << 4) & 4080;

	    output[ooffset+6] = (input[ioffset+2] >>> 8) & 4095;
	    output[ooffset+7] = (input[ioffset+2] >>> 20) & 4095;
	    output[ooffset+8] = (input[ioffset+3] >>> 0) & 4095;
	    output[ooffset+9] = (input[ioffset+3] >>> 12) & 4095;
        output[ooffset+10] = (input[ioffset+3] >>> 24) & 255;
        output[ooffset+10] |= (input[ioffset+4] << 8) & 3840;

        output[ooffset+11] = (input[ioffset+4] >>> 4) & 4095;
        output[ooffset+12] = (input[ioffset+4] >>> 16) & 4095;
        output[ooffset+13] = (input[ioffset+4] >>> 28) & 15;
        output[ooffset+13] |= (input[ioffset+5] << 4) & 4080;

        output[ooffset+14] = (input[ioffset+5] >>> 8) & 4095;
        output[ooffset+15] = (input[ioffset+5] >>> 20) & 4095;
        output[ooffset+16] = (input[ioffset+6] >>> 0) & 4095;
        output[ooffset+17] = (input[ioffset+6] >>> 12) & 4095;
        output[ooffset+18] = (input[ioffset+6] >>> 24) & 255;
        output[ooffset+18] |= (input[ioffset+7] << 8) & 3840;

        output[ooffset+19] = (input[ioffset+7] >>> 4) & 4095;
        output[ooffset+20] = (input[ioffset+7] >>> 16) & 4095;
        output[ooffset+21] = (input[ioffset+7] >>> 28) & 15;
        output[ooffset+21] |= (input[ioffset+8] << 4) & 4080;

        output[ooffset+22] = (input[ioffset+8] >>> 8) & 4095;
        output[ooffset+23] = (input[ioffset+8] >>> 20) & 4095;
        output[ooffset+24] = (input[ioffset+9] >>> 0) & 4095;
        output[ooffset+25] = (input[ioffset+9] >>> 12) & 4095;
        output[ooffset+26] = (input[ioffset+9] >>> 24) & 255;
        output[ooffset+26] |= (input[ioffset+10] << 8) & 3840;

        output[ooffset+27] = (input[ioffset+10] >>> 4) & 4095;
        output[ooffset+28] = (input[ioffset+10] >>> 16) & 4095;
        output[ooffset+29] = (input[ioffset+10] >>> 28) & 15;
        output[ooffset+29] |= (input[ioffset+11] << 4) & 4080;

        output[ooffset+30] = (input[ioffset+11] >>> 8) & 4095;
	    output[ooffset+31] = (input[ioffset+11] >>> 20) & 4095;
	}
	
	public static void main(String args[]){
		//int [] a = {77, 90, 35, 44, 68, 56, 17, 47, 68, 95, 1, 63, 99, 47, 8, 95, 59, 16, 64, 26, 54, 19, 46, 58, 87, 66, 68, 44, 55, 73, 96, 15, 87, 77, 14, 10, 87, 34, 1, 83, 29, 55, 54, 68, 4, 36, 21, 95, 92, 25, 79, 87, 0, 6, 54, 70, 90, 93, 28, 15, 79, 85, 68, 54, 2, 45, 63, 9, 83, 3, 22, 26, 69, 54, 99, 36, 56, 8, 40, 21, 47, 76, 69, 16, 49, 31, 86, 98, 51, 43, 69, 41, 3, 16, 35, 45, 19, 83, 20, 92, 67, 53, 46, 23, 72, 45, 28, 43, 25, 4, 88, 76, 63, 93, 45, 16, 42, 95, 61, 60, 31, 83, 47, 27, 72, 23, 25, 76 };
		
		int a[] = new int[128];
		
		for (int i=0; i<128; i++){
			a[i] = (int)(Math.random() * 128); 
		//	System.out.print(data[i] + " ");
		}
		
		int [] b = new int[128]; int [] c = new int[128];
		
		
		fastNPack(8, 128, 0, 0, a, b);
		
		fastNUnPack(8, 128, 0, 0, b, c);
		
		for (int i=0; i<128; i++){
			if (a[i] != c[i]) System.out.println(i + ": a=" + a[i] + " c=" + c[i] );
		}
		System.out.println("done!");
	}
}