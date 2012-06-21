package com.ntnu.laika.distributed.util;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.ntnu.laika.compression.NewPForDecoding;
import com.ntnu.laika.compression.NewPForEncoding;
import com.ntnu.laika.compression.PackUtils;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class AccumulatorSetCompression {
	public static final byte NOCOMPRESSION = 0;
	public static final byte DOCIDSONLY = 1;
	public static final byte CROPFLOATS = 2;
	public static final byte MAXCOMPRESSION = 3;
	public static int mantissaBits = 16;
	public static int padLimit = 64;
	
	public static LinkedByteBuffer toByteBuffer(AccumulatorSet accs, byte compression){
		LinkedByteBuffer buffer = new LinkedByteBuffer();
				
		int size = accs.size();
		//System.out.println(size+"<<<");
		switch (compression){
			case (NOCOMPRESSION):{
				for (int i=0; i<size; i++){
					buffer.putInt(accs.getDocId());									//store docids
					buffer.putFloat((float)accs.getScore());						//store scores
					accs.next();
				}
			} break;
			case (DOCIDSONLY) :{
				NewPForEncoding encoder = new NewPForEncoding(padLimit);
				
				int diffarray[] = new int[128], prevdocid = 0, curdocid, spos;
	
				for (spos = 0; spos + 128 < size; spos+=128){
					for (int i=0; i<128; i++){										//generate dgaps
						curdocid = accs.getDocId();
						diffarray[i] = curdocid - prevdocid;
						prevdocid = curdocid;
						
						buffer.putFloat((float)accs.getScore());					//store scores
						
						accs.next();
					}
					encoder.encodeSingle(buffer, diffarray, 0, 128);		 		//encode d-gaps
				}
				
				int rest = size - spos;
				for (int i=0; i<rest; i++){											//generate d-gaps
					curdocid = accs.getDocId();
					diffarray[i] = curdocid - prevdocid;
					prevdocid = curdocid;
					
					buffer.putFloat((float)accs.getScore());						//store scores
					
					accs.next();
				}
				
				//if (size < padLimit) for (int i=0; i<rest; i++) diffarray[i]++; //only if it was a single block
				encoder.encodeSingle(buffer, diffarray, 0, rest);
			} break;
			case (CROPFLOATS): {
				int fin[] = new int[128], curbits, bitshift = 23-mantissaBits, eof = mantissaBits << 2;
				int fout[] = new int[eof];

				NewPForEncoding encoder = new NewPForEncoding(padLimit);
				int diffarray[] = new int[128], prevdocid = 0, curdocid, spos;
				
				for (spos = 0; spos + 128 < size; spos+=128){
					for (int i=0; i<128; i++){
						curdocid = accs.getDocId();								//generate d-gaps
						diffarray[i] = curdocid - prevdocid;
						prevdocid = curdocid;
												
						curbits = Float.floatToIntBits((float) accs.getScore());//separate scores
						buffer.put((byte)(curbits >> 23));						//store exponens
						fin[i] = (curbits >> bitshift) & PackUtils.masks[mantissaBits];
						
						accs.next();
					}
					
					PackUtils.cleanArray(fout);									//pack mantissas	
					PackUtils.pack(mantissaBits, 128, 0, 0, fin, fout);
					
					
					for (int i=0; i<eof; i++){
						buffer.putInt(fout[i]);									//store mantissas
					}
					
					encoder.encodeSingle(buffer, diffarray, 0, 128);		 	//encode d-gaps
				}
				
				int rest = size - spos;
				
				for (int i=0; i<rest; i++){
					curdocid = accs.getDocId();								//generate d-gaps
					diffarray[i] = curdocid - prevdocid;
					prevdocid = curdocid;
											
					curbits = Float.floatToIntBits((float) accs.getScore());
					buffer.put((byte)(curbits >> 23));
					fin[i] = (curbits >> bitshift) & PackUtils.masks[mantissaBits];

					accs.next();
				}
				
				for (int i=rest; i<128; i++) fin[i] = 0;					//for the last group we pad zeros.
				PackUtils.cleanArray(fout);
				PackUtils.pack(mantissaBits, 128, 0, 0, fin, fout);
				
				for (int i=0; i<eof; i++){
					buffer.putInt(fout[i]);
				}
				
				//if (size < padLimit) for (int i=0; i<rest; i++) diffarray[i]++; //only if it was a single block
				encoder.encodeSingle(buffer, diffarray, 0, rest);
			} break;
			case (MAXCOMPRESSION) : {
				// PForCompress dgaps
				// PForCompress exponents
				// PForCompress mantissas
			
				NewPForEncoding encoder = new NewPForEncoding(padLimit);
				
				int diffarray[] = new int[128], prevdocid = 0, curdocid, spos;
				int e[] = new int[128], m[] = new int[128], bits, bitshift = 23-mantissaBits;
				
				for (spos = 0; spos + 128 < size; spos+=128){
					for (int i=0; i<128; i++){
						curdocid = accs.getDocId();								//generate d-gaps
						diffarray[i] = curdocid - prevdocid;
						prevdocid = curdocid;
						
						bits =  Float.floatToIntBits((float) accs.getScore());
						e[i] = (bits >> 23) & 0xff;
						m[i] = (bits >> bitshift) & PackUtils.masks[mantissaBits];
						
						accs.next();
					}
					
					encoder.encodeSingle(buffer, e, 0, 128);					//encode exponents
					encoder.encodeSingle(buffer, m, 0, 128);					//encode mantissas
					encoder.encodeSingle(buffer, diffarray, 0, 128);
				}
				
				int rest = size - spos;
				for (int i=0; i<rest; i++){
					curdocid = accs.getDocId();								//generate d-gaps
					diffarray[i] = curdocid - prevdocid;
					prevdocid = curdocid;
					
					bits =  Float.floatToIntBits((float) accs.getScore());
					e[i] = (bits >> 23) & 0xff;
					m[i] = (bits >> bitshift) & PackUtils.masks[mantissaBits];
					
					accs.next();
				}
				
				//if (rest < padLimit) for (int i=0; i<rest; i++) m[i]++;
				encoder.encodeSingle(buffer, e, 0, rest);						//encode exponents
				encoder.encodeSingle(buffer, m, 0, rest);						//encode mantissas
				
				//if (size < padLimit) for (int i=0; i<rest; i++) diffarray[i]++; //only if it was a single block
				encoder.encodeSingle(buffer, diffarray, 0, rest);
			} break;
		}
		
		return buffer;
	}
	
	
	public static void fromByteBuffer(ChannelBuffer buffer, AccumulatorSet accs, int cnt, byte compression){
		//decompress docids
		switch (compression){
			case (NOCOMPRESSION): {
				int docid; double score;
				for (int i=0; i<cnt; i++){
					docid = buffer.readInt();
					score = buffer.readFloat();
					accs.addLast(docid, score);
				}	
			} break;
			case (DOCIDSONLY) :{
				NewPForDecoding decoder = new NewPForDecoding(padLimit);
			
				int diffarray[] = new int[128], prevdocid = 0, spos;
				double drbuffer[] = new double[128];
				
				for (spos = 0; spos + 128 < cnt; spos+=128){
					
					for (int i=0; i<128; i++) drbuffer[i] = buffer.readFloat();
					
					decoder.decodeSingle(buffer, 128, diffarray, 0);
					
					for (int i=0; i<128; i++){						
						diffarray[i] += prevdocid;									
						prevdocid = diffarray[i];
						
						accs.addLast(prevdocid, drbuffer[i]);
					}
				}
				
				int rest = cnt - spos;
				
				for (int i=0; i<rest; i++) drbuffer[i] = buffer.readFloat();
				
				decoder.decodeSingle(buffer, rest, diffarray, 0);
				//if (cnt < padLimit) for (int i=0; i<rest; i++) diffarray[i]--;
				
				for (int i=0; i<rest; i++){							
					diffarray[i] += prevdocid;									
					prevdocid = diffarray[i];
					
					accs.addLast(prevdocid, drbuffer[i]);
				}
			} break;
			case (CROPFLOATS):{
				NewPForDecoding decoder = new NewPForDecoding(padLimit);
				
				int diffarray[] = new int[128], prevdocid = 0, spos;
				
				int fout[] = new int[128], curbits, bitshift = 23-mantissaBits, eof = mantissaBits << 2;
				int fin[] = new int[eof];
				byte exponents[] = new byte[128];
	
				for (spos = 0; spos + 128 < cnt; spos+=128){
					//read exponents
					for (int i=0; i<128; i++){
						exponents[i] = buffer.readByte();
					}
						
					//read mantissas
					for (int i=0; i<eof; i++){
						fin[i] = buffer.readInt();
					}
						
					//unpack mantissas
					PackUtils.unpack(mantissaBits, 128, 0, 0, fin, fout);
					
					//decode dgaps
					decoder.decodeSingle(buffer, 128, diffarray, 0);
					
					//restore docids and scores
					for (int i=0; i<128; i++){
						diffarray[i] += prevdocid;									
						prevdocid = diffarray[i];
						
						curbits = (exponents[i] << 23) & 0x7FFFFFFF;
						curbits |= fout[i] << bitshift;
				
						accs.addLast(prevdocid, Float.intBitsToFloat(curbits));
					}
				}
			
				
				int rest = cnt - spos;
				
				//read exponents
				for (int i=0; i<rest; i++){
					exponents[i] = buffer.readByte();
					//out[i] = buffer.getInt();
				}
				
				//read mantissas
				for (int i=0; i<eof; i++){
					fin[i] = buffer.readInt();
				}
				
				//unpack mantissas
				PackUtils.unpack(mantissaBits, 128, 0, 0, fin, fout);
				
				//decode dgaps
				decoder.decodeSingle(buffer, rest, diffarray, 0);
				//if (cnt < padLimit) for (int i=0; i<rest; i++) diffarray[i]--; //only if it was a single block
				
				//restore floats!
				for (int i=0; i<rest; i++){
					diffarray[i] += prevdocid;									
					prevdocid = diffarray[i];
					
					curbits = (exponents[i] << 23) & 0x7FFFFFFF;
					curbits |= fout[i] << bitshift;
					
					accs.addLast(prevdocid, Float.intBitsToFloat(curbits));
				}
			} break; 
			case (MAXCOMPRESSION):{
				//PForDecompress exponents
				//PForDecompress mantissas
				NewPForDecoding decoder = new NewPForDecoding(padLimit);
				
				int diffarray[] = new int[128], prevdocid = 0, spos;
				int e[] = new int[128], m[] = new int[128], bitshift = 23-mantissaBits;
				
				for (spos = 0; spos + 128 < cnt; spos+=128){
					decoder.decodeSingle(buffer, 128, e, 0);					//decode exponents
					decoder.decodeSingle(buffer, 128, m, 0);					//decode mantissas
					decoder.decodeSingle(buffer, 128, diffarray, 0);			//decode dgaps
					
					for (int i=0; i<128; i++){
						diffarray[i] += prevdocid;									
						prevdocid = diffarray[i];
						
						accs.addLast(prevdocid, Float.intBitsToFloat((e[i] << 23) | (m[i] << bitshift)));
					}
				}
				
				int rest = cnt - spos;
				decoder.decodeSingle(buffer, rest, e, 0);						//decode exponents
				decoder.decodeSingle(buffer, rest, m, 0);						//decode mantissas
				decoder.decodeSingle(buffer, rest, diffarray, 0);				//decode dgaps
		
				//if (rest < padLimit) for (int i=0; i<rest; i++) m[i]--;
				//if (cnt < padLimit) for (int i=0; i<rest; i++) diffarray[i]--;
				
				for (int i=0; i<rest; i++){						
					diffarray[i] += prevdocid;									
					prevdocid = diffarray[i];
				
					accs.addLast(prevdocid, Float.intBitsToFloat((e[i] << 23) | (m[i] << bitshift)));
				}
			} break;
		}
	}
	
	
	public static void main(String[] args) throws NumberFormatException, IOException{
		//BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/test.txt"));
		
		//int num = Integer.parseInt(reader.readLine());
		int num = 10000;
		//String tmp[];
		int docids[] = new int[num];
		double scores[] = new double[num];
		for (int i=0; i<num; i++) {
			//tmp = reader.readLine().split(" ");
			docids[i] = i;//Integer.parseInt(tmp[0]);
			scores[i] = i;//Double.parseDouble(tmp[1]);
		}
		//reader.close();
		
		int odocids[] = new int[num];
		double oscores[] = new double[num];
		
		byte comp = MAXCOMPRESSION;
		for (int i=0; i<100; i++){
			AccumulatorSet inacc = new AccumulatorSet(docids, scores, 0, num);
			AccumulatorSet outacc = new AccumulatorSet(odocids, oscores, 0, 0);
			
			long start = System.currentTimeMillis();
			LinkedByteBuffer buf = toByteBuffer(inacc, comp);
			buf.flip();
			System.out.println(buf.getSize()+ "aasa");
			ChannelBuffer cbuf = ChannelBuffers.buffer(buf.getSize());
			buf.flushToChannelBufferAndFree(cbuf);
			
	//		if (i==99) System.out.println(buf.position());
		
			fromByteBuffer(cbuf, outacc, num, comp);
			for (int j=0; j<num; j++) {
				System.out.println(outacc.getDocId() + " " + outacc.getScore());
				outacc.next();
			}
			long stop = System.currentTimeMillis();
		
			if (i==99) System.out.println("time " + (stop-start));
		}
		
		/*for (int i=0; i<num; i++){
			System.out.println(i + " " + docids[i] + " -> " + odocids[i]);
			System.out.println("----" + (float) scores[i] + " -> " + oscores[i] + " " + (oscores[i]/scores[i]));
		}*/
	}
	
}