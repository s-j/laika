package com.ntnu.laika.structures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Properties;

import com.ntnu.laika.Constants;
import com.ntnu.laika.buffering.BufferPool;
import com.ntnu.laika.structures.docdict.DocDict;
import com.ntnu.laika.structures.docdict.DocDictInputStream;
import com.ntnu.laika.structures.docdict.DocDictOutputStream;
import com.ntnu.laika.structures.docdict.FastDocLength;
import com.ntnu.laika.structures.docdict.ShortDocDict;
import com.ntnu.laika.structures.docdict.ShortDocDictInputStream;
import com.ntnu.laika.structures.docdict.ShortDocDictOutputStream;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScoreInputStream;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScoreOutputStream;
import com.ntnu.laika.structures.lexicon.FastShortLexicon;
import com.ntnu.laika.structures.lexicon.InMemoryLexicon;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconInputStream;
import com.ntnu.laika.structures.lexicon.LexiconOutputStream;
import com.ntnu.laika.structures.lexicon.ShortLexicon;
import com.ntnu.laika.structures.lexicon.ShortLexiconInputStream;
import com.ntnu.laika.structures.lexicon.ShortLexiconOutputStream;
import com.ntnu.laika.structures.postinglist.DiskInvertedIndex;
import com.ntnu.laika.structures.postinglist.PostingListInputStream;
import com.ntnu.laika.structures.postinglist.PostingListOutputStream;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Index implements Closeable{
	protected String path;
	protected Statistics stats;
	protected Properties indexProperties;
	
	protected HashMap<String, Integer> fileNumberCache = new HashMap<String, Integer>();
	protected BufferPool pool;
	
	/**
	 * @deprecated
	 */
	public Index(){}
	
	public Index(String path){
		this.path = path;
		pool = new BufferPool(Constants.TOTAL_BUFFER_BLOCK_SIZE / Constants.BUFFER_BLOCK_SIZE, Constants.BUFFER_BLOCK_SIZE);
		pool.start();
		
		//load statistics
		indexProperties = new Properties();
		try {
			indexProperties.load(new FileInputStream(path + "/index.properties"));
			stats = new Statistics(
				Integer.parseInt(indexProperties.getProperty("index.numberOfDocuments", "0")),
				Integer.parseInt(indexProperties.getProperty("index.numberOfUniqueTerms", "0")),
				Long.parseLong(indexProperties.getProperty("index.numberOfPointers", "0")),
				Long.parseLong(indexProperties.getProperty("index.numberOfTokens", "0")));
			Constants.USE_SKIPS = Boolean.parseBoolean(
					indexProperties.getProperty("index.useSkips",""+Constants.USE_SKIPS));
			Constants.STRING_BYTE_LENGTH = Integer.parseInt(
					indexProperties.getProperty("index.maxTermLength",""+Constants.STRING_BYTE_LENGTH));
			Constants.DOCNO_BYTE_LENGTH = Integer.parseInt(
					indexProperties.getProperty("intdex.maxDocnoLength",""+Constants.DOCNO_BYTE_LENGTH));
			Constants.WORKERS_CNT = Integer.parseInt(indexProperties.getProperty("workerscnt",""+Constants.WORKERS_CNT));
		} catch (FileNotFoundException e) {
			stats = new Statistics(0, 0, 0, 0);
		} catch (IOException e) {
			e.printStackTrace();
			stats = new Statistics(0, 0, 0, 0);
		}
	}
	
	protected Integer getFileNumber(String filename){
		Integer fileNumber = fileNumberCache.get(filename);
		if (fileNumber!=null) return fileNumber;
		
        try {
    	    File file = new File(filename);
    	    file.createNewFile();
            FileChannel indexChannel = new RandomAccessFile(file, "rw").getChannel();
			fileNumber = pool.registerFile(indexChannel, file);
			fileNumberCache.put(filename, fileNumber);
			return fileNumber;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	protected BufferWrapper getBufferFromFile(String filename){
		FileWrapper fw = null;
        try {
    	    fw = new FileWrapper(pool, getFileNumber(filename));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		return fw;
	}
	
	/*
	 * Statistics 
	 */
	public Statistics getStatistics(){
		return stats;
	}
	
	public void setStatistics(Statistics newStats){
		stats = newStats;
	}

	
	/*
	 * Lexicon Structures
	 */
	public Lexicon getLexicon(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.lex"));
			return new Lexicon(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Lexicon getInMemoryLexicon(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.lex"));
			return new InMemoryLexicon(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public LexiconInputStream getLexiconInputStream(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.lex"));
			return new LexiconInputStream(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public LexiconOutputStream getLexiconOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.lex"));
			return new LexiconOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ShortLexicon getShortLexicon(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.slex"));
			return new ShortLexicon(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ShortLexiconInputStream getShortLexiconInputStream(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.slex"));
			return new ShortLexiconInputStream(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public ShortLexiconOutputStream getShortLexiconOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.slex"));
			return new ShortLexiconOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public void loadFastShortLexicon(int totEntries){
		FastShortLexicon.setEntries(totEntries, getShortLexiconInputStream(totEntries));
	}
	
	/*
	 * DocDict Structures
	 */
	
	public DocDict getDocDict(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.doc"));
			return new DocDict(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public DocDictInputStream getDocDictInputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.doc"));
			return new DocDictInputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public DocDictOutputStream getDocDictOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.doc"));
			return new DocDictOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public void loadFastDocLengths(int totEntries){
		FastDocLength.setLengths(totEntries, getDocDictInputStream());
	}
	
	public ShortDocDict getShortDocDict(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.sdoc"));
			return new ShortDocDict(buffer, totEntries);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ShortDocDictInputStream getShortDocDictInputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.sdoc"));
			return new ShortDocDictInputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public ShortDocDictOutputStream getShortDocDictOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.sdoc"));
			return new ShortDocDictOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public void loadFastShortDocLengths(int totEntries){
		FastDocLength.setLengths(totEntries, getShortDocDictInputStream());
	}
	
	/*
	 * Inverted Index
	 */
	public DiskInvertedIndex getInvertedIndex(){
		return new DiskInvertedIndex(pool, getFileNumber(path+"/index.inv"));
	}
	
	public PostingListOutputStream getPostingListOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.inv"));
			return new PostingListOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public PostingListInputStream getPostingListInputStream(int totEntries){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.inv"));
			return new PostingListInputStream(buffer, getLexiconInputStream(totEntries));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * FastMaxScore streams
	 */
	public FastMaxScoreOutputStream getFastMaxScoreOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.fms"));
			return new FastMaxScoreOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public FastMaxScoreInputStream getFastMaxScoreInputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.fms"));
			return new FastMaxScoreInputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void loadFastMaxScores(int totEntries){		
		FastMaxScore.setMaxScores(totEntries, totEntries, getFastMaxScoreInputStream());
	}
	
	public void loadFastMaxScores(int totEntries, int readEntries){		
		FastMaxScore.setMaxScores(totEntries, readEntries, getFastMaxScoreInputStream());
	}
	
	/*
	 * Store statistics
	 */
	protected void storeStatsAndSettings(){		
		indexProperties.setProperty("index.numberOfDocuments", ""+stats.getNumberOfDocuments());
		indexProperties.setProperty("index.numberOfUniqueTerms", ""+stats.getNumberOfUniqueTerms());
		indexProperties.setProperty("index.numberOfPointers", ""+stats.getNumberOfPointers());
		indexProperties.setProperty("index.numberOfTokens", ""+stats.getNumberOfTokens());
		indexProperties.setProperty("index.useSkips",""+Constants.USE_SKIPS);
		indexProperties.setProperty("index.maxTermLength",""+Constants.STRING_BYTE_LENGTH);
		indexProperties.setProperty("intdex.maxDocnoLength",""+Constants.DOCNO_BYTE_LENGTH);
		try {
			indexProperties.store(new FileOutputStream(path + "/index.properties"),"");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public Properties getIndexProperties(){
		return indexProperties;
	}
	
	public String getPath() {
		return path;
	}
	
	@Override
	public void close() {
		storeStatsAndSettings();
		pool.stop();
	}
}
