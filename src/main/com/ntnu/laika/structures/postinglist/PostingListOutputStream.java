package com.ntnu.laika.structures.postinglist;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ntnu.laika.Constants;
import com.ntnu.laika.compression.NewPForEncoding;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class PostingListOutputStream implements Closeable{
	private LevelWriter[] levelwriters;
	private NewPForEncoding encoder;
	
	private ByteBuffer chunkbuffer;
	private int[] chunklengths;
	private int writeptr = 0;
	private int readptr = 0;
	
	private BufferWrapper outwriter;
	private int currentMaxLevel;
	
	private boolean use_skips;
	
    public PostingListOutputStream(BufferWrapper buffer) throws IOException, InterruptedException {
        outwriter = buffer;
        chunkbuffer = ByteBuffer.allocateDirect(8 * Constants.MAX_NUMBER_OF_DOCUMENTS);
        chunklengths = new int[(Constants.MAX_NUMBER_OF_DOCUMENTS >> 7) + 1];
        
        use_skips = Constants.USE_SKIPS;
        if (use_skips){
        	int maxlevels = skipLevelsRequired(Constants.MAX_NUMBER_OF_DOCUMENTS);
        	int postingsExpected = Constants.MAX_NUMBER_OF_DOCUMENTS;
        	levelwriters = new LevelWriter[maxlevels];
        	for (int i=0; i < maxlevels; i++){
        		postingsExpected = (postingsExpected >> 7) + 1;
        		levelwriters[i] = new LevelWriter(i,
        				ByteBuffer.allocateDirect(8 * postingsExpected),
        				new int[(postingsExpected >> 7) + 1]);
        	//	System.out.println(i+ ":" + postingsExpected);
        	}
        }
        
        encoder = new NewPForEncoding();
    }
    
	/**
	 * Calculates the number of skip-block levels
	 * 0 to 128 entries will require 0 skip levels,
	 * 128 to 16384 will require 1 skip level,
	 * 16384 to 2097152 - 2 skip levels, etc.
	 *  
	 * @param postings - number of postings to be stored 
	 * @return number of skip levels
	 */
    public static int skipLevelsRequired(int postings){
    	int ret = 0;
    	while ( 1<<(7*ret + 7) < postings ) ret++;
    	return ret;
    }
    
	private class LevelWriter{
		int level;
		ByteBuffer bytebuffer;
		int[] lenarray;
		int[] docids = new int[128];
		int[] offsets = new int[128];
		int lastdocid =0, lastoffset = 0, writeptr = 0, readptr = 0, lastpos = 0, iptr = 0;
		//int _entries = 0;
		
		LevelWriter(int level, ByteBuffer bytebuffer, int[] lenarray){
			this.level = level;
			this.bytebuffer = bytebuffer;
			this.lenarray = lenarray;
		}
		
		/**
		 * Adds a skipping entry to this level. For each 128 entries, or when forced, the method writes a new chunk to 
		 * the internal buffer. In this case, the last docid and endoffsetptr are sent to the next skip level.
		 * @param docid
		 * @param offset
		 * @param force
		 */
		
		void addEntry(int docid, int offset, boolean force){
			docids[iptr] = docid - lastdocid;
			lastdocid = docid;
			offsets[iptr] = offset - lastoffset;
			lastoffset = offset;
			
			if (++iptr == 128 | force){
				encoder.encodeSingle(bytebuffer, docids, 0, iptr);
				encoder.encodeSingle(bytebuffer, offsets, 0, iptr);
				int newpos = bytebuffer.position();
				lenarray[writeptr] = newpos - lastpos;
				lastpos = newpos;

				writeptr++;				
				
				if (level < currentMaxLevel)
					levelwriters[level + 1].addEntry(docid, offset + newpos, force);
				
				iptr = 0;
			}
			//_entries++;
		}
		
		/**
		 * Flushes skip-chunks from current readptr and up to max(limit, writeptr),
		 * recursively calls fluskSkipChunks(readptr * 128) one level down for each flushed chunk,
		 * or flushChunks(readptr * 128) if this is the lowest level (0). 
		 *  
		 * @param limit 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		void flushSkipChunks(int limit) {
			if (limit > writeptr) limit = writeptr;
			
			while (readptr < limit){
				//write chunk-pair number ptr
				
				outwriter.put(bytebuffer, lenarray[readptr]);
				
				if (level > 0) {
					levelwriters[level-1].flushSkipChunks((readptr+1)<<7);
				} else {
					flushChunks((readptr+1)<<7);
				}
				readptr++;
			}
		}
		
		/**
		 * resets all the internal pointers and counters
		 */
		void reset(){
			lastdocid =0; lastoffset = 0; writeptr = 0;
			readptr = 0; lastpos = 0; iptr = 0;
			//_entries = 0;
			bytebuffer.position(0);
		}
	}
	
	/**
	 * Flushes chunks from current readptr and up to max(limit, writeptr),
	 * 
	 * @param limit
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void flushChunks(int limit) {
		if (limit > writeptr) limit = writeptr;
		
		while (readptr < limit){
			outwriter.put(chunkbuffer, chunklengths[readptr]);									//flush chunk number ptr
			readptr++;
		}
	}
	
	/**
	 * Main flush method, calls either the specified toplevel writer, or flushChunks alone, if toplevel < 0.
	 * The method also resets all the internal counters and pointers for internal buffers
	 * 
	 * @param toplevel
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void flush(int toplevel) {
		chunkbuffer.position(0);													   //reset all buffer positions to 0
		for (LevelWriter lw : levelwriters){
			lw.bytebuffer.position(0);
		}
		
		if (toplevel < 0) flushChunks(Integer.MAX_VALUE);
		else levelwriters[toplevel].flushSkipChunks(Integer.MAX_VALUE);
		
		//reset chunkbuffer and skip writers
		readptr = 0; writeptr = 0; chunkbuffer.position(0);
		for (LevelWriter lw : levelwriters)	lw.reset();
	}
	
	private void flush() {
		chunkbuffer.position(0);													   //reset all buffer positions to 0
		flushChunks(Integer.MAX_VALUE);
		readptr = 0; writeptr = 0; chunkbuffer.position(0);
	}
			
	
	//track term id and starts/ends of posting lists 	
	private int termId = 0;
	private long lastposition = 0;
	
	/**
	 * @param term
	 * @param n_t
	 * @param TF
	 * @param scores
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public LexiconEntry nextEntry(String term, int n_t, int TF, int[][] scores) {
		int numentries = n_t;
		currentMaxLevel =  skipLevelsRequired(n_t) - 1;
		//System.out.println(currentMaxLevel+"<<<" + n_t);
		boolean _useskips = currentMaxLevel > -1;
		
		int spos, pos, lastpos = 0;
		int diffarray[] = new int[128], prevdocid = 0, curdocid;
		
		for (spos=0; spos + 128 < numentries; spos+=128){
			for (int i=0; i<128; i++){																//generate dgaps
				curdocid = scores[0][spos+i];
				diffarray[i] = curdocid - prevdocid;
				prevdocid = curdocid;
			}
			encoder.encodeSingle(chunkbuffer, diffarray, 0, 128);									//encode d-gaps
			encoder.encodeSingle(chunkbuffer, scores[1], spos, 128);								//encode frequencies
			pos =  chunkbuffer.position();
			chunklengths[writeptr] = pos - lastpos;													//store chunklength
			lastpos = pos;
			writeptr++;																				//incr. writeptr
			
			if (use_skips & _useskips) levelwriters[0].addEntry(scores[0][spos+127], lastpos, false);			//update skips
		}
		
		//encode the last chunk, force level writers to finish their chunks!
		int rest = numentries - spos;
		for (int i=0; i<rest; i++){																	// generate d-gaps
			curdocid = scores[0][spos+i];
			diffarray[i] = curdocid - prevdocid;
			prevdocid = curdocid;
		}
		encoder.encodeSingle(chunkbuffer, diffarray, 0, rest);										//same as main loop
		encoder.encodeSingle(chunkbuffer, scores[1], spos, rest);
		pos =  chunkbuffer.position();
		chunklengths[writeptr] = pos - lastpos;
		lastpos = pos;
		writeptr++;
		
		if (use_skips & _useskips){
			levelwriters[0].addEntry(scores[0][numentries-1], lastpos, true);
			//flush writers to file and reset.
			flush(currentMaxLevel); 
		} else {
			flush();
		}
		
	
		//store end position, return lexicon entry, increment termId
		long newpos =  outwriter.position();
		LexiconEntry ret = new LexiconEntry(term, termId++, n_t, TF, lastposition, newpos);
		lastposition = newpos;
		return ret;
	}

	@Override
	public void close() {
		outwriter.close();
	}
}
