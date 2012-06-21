package com.ntnu.laika.compression;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>, <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>, 
 * @version $Id $.
 */
public class NewPFor {
	private static int[] maxCodes = new int[33];
	
	static {
		for (int i = 0; i < 33; i++){
			maxCodes[i] = (int)((1 << i) - 1);
		}
	}
	
    public static int encode(int width, int frameOfReference,
    		int[] input, int[] intermediateBuffer, int inputOffset, 
            int[] output, int[] exceptionIndexes, int[] exceptions){
            int maxCode = maxCodes[width];
            int j = 0;												//number of exceptions
            for (int i = 0; i < 128; i++){
            	// TODO: Double cursor?
                int value = input[inputOffset + i] - frameOfReference;
                
                intermediateBuffer[i] = value & maxCode;			//store lower width bits
                exceptionIndexes[j] = i;							//exception index
                exceptions[j] = value >>> width;
			
                if (exceptions[j] > 0){								//check if this is an exception
                    j++;
                }

                /* this is slower than using IF
                bool exception = exceptions[j] > 0;
                j += *(byte*)&(exception) & 1;
                 */
            }

            for (int i = j - 1; i > 0; i--){
                exceptionIndexes[i] -= exceptionIndexes[i - 1] + 1;//exception offset
            }
            exceptionIndexes[j] = 0; // reset the last index as there is no exception
            //System.out.println("bw: " + width + " for: " + frameOfReference + " exc: " + j);
            FastPack.fast128Pack(width, 0, 0, intermediateBuffer, output);
            return j;
        }

        public static void decode(int n, int width, int frameOfReference,
        		int numberOfExceptions, int[] encoded, int[] decoded,
        		int[] exceptions, int[] exceptionIndexes, int encodedOffset, int decodedOffset){
     
            FastPack.fast128UnPack(width, encodedOffset, decodedOffset, encoded, decoded);
            //for (int i=0; i< 128; i++) System.out.println(i + ":" + decoded[i]);
            
            int exceptionIndex = -1;
            for (int i = 0; i < numberOfExceptions; i++){
                exceptionIndex += (int)exceptionIndexes[i] + 1;
                decoded[decodedOffset + exceptionIndex] |= exceptions[i] << width;
            }

            if (frameOfReference > 0){
                for (int i = decodedOffset; i < decodedOffset+128; i++){
                    decoded[i] += frameOfReference;
                }
            }
        }
}


