package com.ntnu.laika.structures.postinglist;

import com.ntnu.laika.Constants;
import com.ntnu.laika.compression.NewPForDecoding;
import com.ntnu.laika.query.processing.scoremodels.BM25;
import com.ntnu.laika.runstats.SimpleStats;
import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.structures.docdict.FastDocLength;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DefaultPostingListIterator implements PostingListIterator {
	
	private BufferWrapper buffer;
	private NewPForDecoding decoder;
	
	private LevelReader[] levelreaders;
	private final boolean use_skips;
	private int maxSkipLevel;
	
	private class LevelReader{
		private int level;
		
		private int[] gaps = new int[128];
		private long[] offsets = new long[129];
		private int[] docids = new int[129];
		
		private int chunkno = 0, iptr = 0;
		
		private int lastchunk, lastposting, maxdocid;
		
		LevelReader(int level, int nt){
			this.level = level;
			 			
			lastchunk = nt >> 7;
			lastposting = nt - (lastchunk << 7);
			if (lastposting > 0) {
				lastposting--;
			} else {
				lastposting = 127;
				lastchunk--;
			}

			if (level < maxSkipLevel){
				levelreaders[level + 1] = new LevelReader(level + 1, lastchunk + 1);
			}
		}

		boolean readNextChunk(){
			if (chunkno >= lastchunk) return false;
			
			if (level < maxSkipLevel){
				LevelReader lr = levelreaders[level + 1];
				if (lr.next()){
					readChunk(lr.getOffset(), lr.getRefDocId(), lr.getChildChunkNo());
					return true;
				}
			} else {
				readChunk(offsets[128], docids[128], chunkno + 1);
				return true;
			}
			
			return false;
		}
		
		void readChunk(long position, int refdocid, int chunkno){
			buffer.position(position);
			readChunk(refdocid, chunkno);
		}
		
		void readChunk(int refdocid, int chunkno){
			//SimpleStats.addDescription(1, 2);
			
			int postings = (chunkno < lastchunk) ? 128 : (lastposting + 1);
			
			decoder.decodeSingle(buffer, postings, docids, 1);
			decoder.decodeSingle(buffer, postings, gaps, 0);			
			
			offsets[0] = buffer.position();
			docids[0] = refdocid;

			for (int i=0; i<postings; i++) {
				docids[i+1] += docids[i];
				offsets[i+1] = gaps[i] + offsets[i];
			}

			this.chunkno = chunkno;
			iptr = 0;
			maxdocid = docids[postings];
		}
		
		boolean next(){
			int _lastposting = (chunkno < lastchunk) ? 127 : lastposting;
			if (iptr < _lastposting){
				iptr++;
				return true;
			}
			if (readNextChunk()){
				return true;
			}
			return false;
		}
			
		boolean skipTo(int docid){
			if (docid <= docids[iptr+1]) return true;

			if (docid > maxdocid) {
				if (level < maxSkipLevel) {
					LevelReader lr = levelreaders[level + 1];
					if (lr.skipTo(docid)){
						readChunk(lr.getOffset(), lr.getRefDocId(), lr.getChildChunkNo());
					} else {
						return false;
					}
				} else { //iterate nextChunk until EOF or found
					while (docid > maxdocid) {
						if ( !readNextChunk()) return false;
					}
				}
			}
			
			int postings = (chunkno < lastchunk) ? 128 : (lastposting + 1);
			for (; iptr < postings; iptr++){
				if (docid <= docids[iptr + 1]){
					return true;
				}
			}
			return false;
		}
		
		private int getChildChunkNo() {
			return chunkno * 128 + iptr;
		}
		
		int getRefDocId(){
			return docids[iptr];
		}
		
		long getOffset(){
			return offsets[iptr];
		}
	}
	
	
	
	private int[] docids = new int[128];
	private int[] freqs = new int[128];
	private int iptr = 0, chunkno = 0, maxdocid;
	private int lastchunk, lastposting; 
	
	private long startOffset;
	private int nt;
	
	public DefaultPostingListIterator(BufferWrapper buffer, int nt, long startOffset){
		this.buffer = buffer;
		decoder = new NewPForDecoding();
		
		lastchunk = nt >> 7;
		lastposting = nt - (lastchunk << 7);
		if (lastposting > 0) {
			lastposting--;
		} else {
			lastposting = 127;
			lastchunk--;
		}
		
		use_skips = Constants.USE_SKIPS & (nt > 128); 
		
		this.startOffset = startOffset;
		this.nt = nt;
		buffer.position(startOffset);
		if (use_skips){
			int _skiplevels = PostingListOutputStream.skipLevelsRequired(nt);
			levelreaders = new LevelReader[_skiplevels];
			maxSkipLevel = _skiplevels - 1;
			levelreaders[0] = new LevelReader(0, lastchunk + 1);
			
			//decode first skip blocks
			for (int i = maxSkipLevel; i >= 0; i--){
				levelreaders[i].readChunk(0,0);
			}
		}

		//decode the first block itself
		readChunk(0, 0);
	}

	/**
	 * Reads next chunk if possible
	 * @return false if there are no more chunks left
	 */
	private boolean readNextChunk(){
		if (chunkno >= lastchunk) return false;
		
		if (use_skips){
			LevelReader lr = levelreaders[0];
			if (lr.next()){
				readChunk(lr.getOffset(), lr.getRefDocId(), lr.getChildChunkNo());
				return true;
			}
		} else {
			readChunk(docids[127], chunkno + 1);
			return true;
		}
		return false;
	}
	
	/**
	 * Read a chunk at specified position
	 * @param position chunk start
	 * @param refdocid reference DocId 
	 * @param chunkno sequential chunk number
	 */
	private void readChunk(long position, int refdocid, int chunkno){
		buffer.position(position);
		readChunk(refdocid, chunkno);
	}
	private boolean readFreqChunk = false;
		
	/**
	 * Read a chunk from the buffer (assumed that the pointer is aligned)
	 * @param refdocid reference DocId
	 * @param chunkno sequential chunk number
	 */
	private void readChunk(int refdocid, int chunkno){
		int postings = (chunkno < lastchunk) ? 128 : (lastposting + 1); 

		decoder.decodeSingle(buffer, postings, docids, 0);
		readFreqChunk = !use_skips;
		if (readFreqChunk){
			decoder.decodeSingle(buffer, postings, freqs, 0);
		//	SimpleStats.addDescription(1, 2);
		} else {
		//	SimpleStats.addDescription(1, 1);
		}
		
		for (int i=0; i<postings; i++) {
			docids[i] += refdocid;
			refdocid = docids[i];
		}
		
		this.chunkno = chunkno;
		iptr = 0;
		maxdocid = refdocid;
	}
	
	private void readFreqChunk(){
		decoder.decodeSingle(buffer, (chunkno < lastchunk) ? 128 : (lastposting + 1), freqs, 0);
		//  SimpleStats.addDescription(1, 1);
		readFreqChunk = true;
	}
	
	
	//boolean seen_d = false;
	
	/**
	 * Skips to the required entry or the first entry larger than the required
	 * @return false if EOF
	 */
	@Override
	public boolean skipTo(int docid){
		if (docid <= docids[iptr]) return true;
		
		//seen_d = false;
		if (docid > maxdocid) {
			if (use_skips) {
				LevelReader lr = levelreaders[0];
				if (lr.skipTo(docid)){
					readChunk(lr.getOffset(), lr.getRefDocId(), lr.getChildChunkNo());
				} else {
					return false;
				}
			} else { //iterate nextChunk until EOF or found
				while (docid > maxdocid) {
					if ( !readNextChunk()) return false;
				}
			}
		}
		
		int postings = (chunkno < lastchunk) ? 128 : (lastposting + 1);
		for (; iptr<postings; iptr++){
			if (docid <= docids[iptr]){
				return true;
			}
		}
		
		return false;
	}
		
	/**
	 * Fetches the next entry
	 * @return false if EOF
	 */
	@Override
	public boolean next(){
	//	seen_d = false;
		int _lastposting = (chunkno < lastchunk) ? 127 : lastposting;
		if (iptr < _lastposting){
			iptr++;
			return true;
		}
		if (readNextChunk()){
			return true;
		}
		return false;
	}
	
	/**
	 * Returns current docID
	 * @return
	 */
	public final int getDocId(){
		/*if (!seen_d){
			SimpleStats.addDescription(2, 1);
			seen_d = true;
		}*/
		return docids[iptr];
	}
	
	/**
	 * returns current frequency
	 * @return
	 */
	public final int getFrequency(){
		if (!readFreqChunk) readFreqChunk();
		return freqs[iptr];
	}

	@Override
	public void close() {
		buffer.close();
	}
	
	public void reset() {
		buffer.position(startOffset);
		if (use_skips){
			int _skiplevels = PostingListOutputStream.skipLevelsRequired(nt);
			levelreaders = new LevelReader[_skiplevels];
			maxSkipLevel = _skiplevels - 1;
			levelreaders[0] = new LevelReader(0, lastchunk + 1);
			
			//decode first skip blocks
			for (int i = maxSkipLevel; i >= 0; i--){
				levelreaders[i].readChunk(0,0);
			}
		}
		//decode the first block itself
		readChunk(0, 0);
	}
	
	private double precomputed;
	private BM25 wmodel = null;
	
	public void prepareForScoring(double keyFreq, double docFreq, double termFreq,
			double numOfDocs, double avgDocLength){
		wmodel = new BM25();
		wmodel.setKeyFrequency(keyFreq);
		wmodel.setDocumentFrequency(docFreq);
		wmodel.setTermFrequency(termFreq);
		wmodel.setNumberOfDocuments(numOfDocs);
		wmodel.setAverageDocumentLength(avgDocLength);
		precomputed = wmodel.precompute();
	}
	
	@Override
	public double getScore() {
	//	SimpleStats.addDescription(3, 1);
		if (!readFreqChunk) readFreqChunk();
		return wmodel.score(freqs[iptr], FastDocLength.getNumberOfTokens(docids[iptr]), precomputed);
	}
	
	public double getFakeScore(int freq, double doclength){
	//	SimpleStats.addDescription(3, 1);
		return wmodel.score(freq, doclength, precomputed);
	}
}
