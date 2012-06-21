package com.ntnu.laika.distributed.util;

import java.nio.ByteBuffer;

import com.ntnu.laika.compression.NewPForDecoding;
import com.ntnu.laika.compression.NewPForEncoding;
import com.ntnu.laika.compression.PackUtils;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.processing.ResultHeap;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ResultSetCompression {
	public static final byte NOCOMPRESSION = 0;
	public static final byte DOCIDSONLY = 1;
	public static final byte CROPFLOATS = 2;
	public static final byte MAXCOMPRESSION = 3;
	public static int mantissaBits = 23;
	public static int padLimit = 64;
	
	public static LinkedByteBuffer toByteBuffer(ResultHeap rheap, byte compression){
		int size = rheap.size();
		int[] docids = new int[size];
		double[] scores = new double[size];
		rheap.decrSortResults(docids, scores);
		
		LinkedByteBuffer buffer = new LinkedByteBuffer();
				
		if (compression == NOCOMPRESSION){
			for (int i=0; i<size; i++){
				buffer.putInt(docids[i]);							//store docids
				buffer.putFloat((float)scores[i]);					//store scores
			}
		} else if (compression == DOCIDSONLY){
			NewPForEncoding encoder = new NewPForEncoding(padLimit);
			int spos;

			for (spos = 0; spos + 128 < size; spos+=128){
				for (int i=0; i<128; i++){															
					buffer.putFloat((float) scores[spos+i]);		//store scores
				}
				encoder.encodeSingle(buffer, docids, spos, 128);	//encode docids
			}
			
			int rest = size - spos;
			for (int i=0; i<rest; i++){									
				buffer.putFloat((float)scores[spos+i]);				//store scores
			}
			encoder.encodeSingle(buffer, docids, spos, rest);
		} else if (compression == CROPFLOATS){
				int fin[] = new int[128], curbits, bitshift = 23-mantissaBits, eof = mantissaBits << 2;
				int fout[] = new int[eof];
				double tmp, prev = size > 0 ? scores[0] : 0;
				
				NewPForEncoding encoder = new NewPForEncoding(padLimit);
				int spos;
				
				for (spos = 0; spos + 128 < size; spos+=128){
					for (int i=0; i<128; i++){
						tmp = scores[spos+i];
						curbits = Float.floatToIntBits((float) (prev-tmp));	//separate scores
						prev = tmp;
						buffer.put((byte)(curbits >> 23));					//store exponens
						fin[i] = (curbits >> bitshift) & PackUtils.masks[mantissaBits];
					}
					
					PackUtils.cleanArray(fout);								//pack mantissas	
					PackUtils.pack(mantissaBits, 128, 0, 0, fin, fout);
					
					for (int i=0; i<eof; i++){
						buffer.putInt(fout[i]);								//store mantissas
					}
					
					encoder.encodeSingle(buffer, docids, spos, 128);		//encode docs
				}
				
				int rest = size - spos;
				
				for (int i=0; i<rest; i++){											
					tmp = scores[spos+i];
					curbits = Float.floatToIntBits((float) (prev-tmp));		//calculate differences, separate scores
					prev = tmp;
					buffer.put((byte)(curbits >> 23));
					fin[i] = (curbits >> bitshift) & PackUtils.masks[mantissaBits];
				}
				
				for (int i=rest; i<128; i++) fin[i] = 0;					//for the last group we pad zeros.
				PackUtils.cleanArray(fout);
				PackUtils.pack(mantissaBits, 128, 0, 0, fin, fout);
				
				for (int i=0; i<eof; i++){
					buffer.putInt(fout[i]);
				}
				encoder.encodeSingle(buffer, docids, spos, rest);
				
		} else if (compression ==  MAXCOMPRESSION){
				// PForCompress dgaps
				// PForCompress exponents
				// PForCompress mantissas
			
				NewPForEncoding encoder = new NewPForEncoding(padLimit);
				
				int spos;
				int e[] = new int[128], m[] = new int[128], bits, bitshift = 23-mantissaBits;
				double tmp, prev = size > 0 ? scores[0] : 0;
				
				for (spos = 0; spos + 128 < size; spos+=128){
					for (int i=0; i<128; i++){
						tmp = scores[spos+i];
						bits = Float.floatToIntBits((float) (prev-tmp));		//calculate differences, separate scores
						prev = tmp;
						e[i] = (bits >> 23) & 0xff;
						m[i] = (bits >> bitshift) & PackUtils.masks[mantissaBits];
					}
					
					encoder.encodeSingle(buffer, e, 0, 128);					//encode exponents
					encoder.encodeSingle(buffer, m, 0, 128);					//encode mantissas
					encoder.encodeSingle(buffer, docids, spos, 128);
				}
				
				int rest = size - spos;
				for (int i=0; i<rest; i++){
					tmp = scores[spos+i];
					bits = Float.floatToIntBits((float) (prev-tmp));		//calculate differences, separate scores
					prev = tmp;
					e[i] = (bits >> 23) & 0xff;
					m[i] = ((bits >> bitshift) + 1) & PackUtils.masks[mantissaBits];
				}
				
				if (rest < padLimit) for (int i=0; i<rest; i++) m[i]++;
				encoder.encodeSingle(buffer, e, 0, rest);						//encode exponents
				encoder.encodeSingle(buffer, m, 0, rest);						//encode mantissas
				encoder.encodeSingle(buffer, docids, spos, rest);
		}
		
		return buffer;
	}
	
	
	public static QueryResults fromByteBuffer(ByteBuffer buffer, int cnt, int realCnt, byte compression){
		int[] docids = new int[cnt];
		double[] scores = new double[cnt];
		
		//decompress docids
		if (compression == NOCOMPRESSION){
			for (int i=0; i<cnt; i++){
				docids[i] = buffer.getInt();
				scores[i] = buffer.getFloat();
			}
		} else if (compression == DOCIDSONLY) {
			NewPForDecoding decoder = new NewPForDecoding(padLimit);
			
			double drbuffer[] = new double[128];
			int spos;
			for (spos = 0; spos + 128 < cnt; spos+=128){
				for (int i=0; i<128; i++) scores[spos+i] = buffer.getFloat();
				decoder.decodeSingle(buffer, 128, docids, spos);
			}
			
			int rest = cnt - spos;
			for (int i=0; i<rest; i++) drbuffer[i] = buffer.getFloat();			
			decoder.decodeSingle(buffer, rest, docids, spos);

		} else if (compression == CROPFLOATS){
			NewPForDecoding decoder = new NewPForDecoding(padLimit);
			
			int spos;
			
			int fout[] = new int[128], curbits, bitshift = 23-mantissaBits, eof = mantissaBits << 2;
			int fin[] = new int[eof];
			byte exponents[] = new byte[128];
			double last = 0d, diff;
			
			for (spos = 0; spos + 128 < cnt; spos+=128){
				//read exponents
				for (int i=0; i<128; i++){
					exponents[i] = buffer.get();
				}
					
				//read mantissas
				for (int i=0; i<eof; i++){
					fin[i] = buffer.getInt();
				}
					
				//unpack mantissas
				PackUtils.unpack(mantissaBits, 128, 0, 0, fin, fout);
				
				//decode dgaps
				decoder.decodeSingle(buffer, 128, docids, spos);
				
				//restore docids and scores
				for (int i=0; i<128; i++){
					curbits = (exponents[i] << 23) & 0x7FFFFFFF;
					curbits |= fout[i] << bitshift;
					diff = Float.intBitsToFloat(curbits);
					last = scores[spos+i] = (spos > 0 || i>0 ) ? last - diff : diff;
				}
			}
				
			int rest = cnt - spos;
			
			//read exponents
			for (int i=0; i<rest; i++){
				exponents[i] = buffer.get();
				//out[i] = buffer.getInt();
			}
				
			//read mantissas
			for (int i=0; i<eof; i++){
				fin[i] = buffer.getInt();
			}
				
			//unpack mantissas
			PackUtils.unpack(mantissaBits, 128, 0, 0, fin, fout);
			
			//decode dgaps
			decoder.decodeSingle(buffer, rest, docids, spos);
				
			//restore floats!
			for (int i=0; i<rest; i++){

				curbits = (exponents[i] << 23) & 0x7FFFFFFF;
				curbits |= fout[i] << bitshift;
					
				diff = Float.intBitsToFloat(curbits);
				last = scores[spos+i] = (spos > 0 || i>0 ) ? last - diff : diff;
			}
		} else if (compression ==  MAXCOMPRESSION){
				//PForDecompress exponents
				//PForDecompress mantissas
				NewPForDecoding decoder = new NewPForDecoding(padLimit);
				
				int spos, e[] = new int[128], m[] = new int[128], bitshift = 23-mantissaBits;
				double diff, last = 0d;
				
				for (spos = 0; spos + 128 < cnt; spos+=128){
					decoder.decodeSingle(buffer, 128, e, 0);					//decode exponents
					decoder.decodeSingle(buffer, 128, m, 0);					//decode mantissas
					decoder.decodeSingle(buffer, 128, docids, spos);			//decode docids
					
					for (int i=0; i<128; i++){
						diff = Float.intBitsToFloat((e[i] << 23) | (m[i] << bitshift));
						last = scores[spos+i] = (spos > 0 || i>0 ) ? last - diff : diff;
					}
				}
				
				int rest = cnt - spos;
				decoder.decodeSingle(buffer, rest, e, 0);						//decode exponents
				decoder.decodeSingle(buffer, rest, m, 0);						//decode mantissas
				decoder.decodeSingle(buffer, rest, docids, spos);				//decode docids
		
				if (rest < padLimit) for (int i=0; i<rest; i++) m[i]--;
				
				for (int i=0; i<rest; i++){						
					diff = Float.intBitsToFloat((e[i] << 23) | (m[i] << bitshift));
					last = scores[spos+i] = (spos > 0 || i>0 ) ? last - diff : diff;
				}
		}
		
		return new QueryResults(docids, scores, cnt, realCnt);
	}
	
}