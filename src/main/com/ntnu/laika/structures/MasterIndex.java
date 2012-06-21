package com.ntnu.laika.structures;

import java.io.IOException;

import com.ntnu.laika.structures.lexicon.global.GlobalLexicon;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconInputStream;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconOutputStream;
import com.ntnu.laika.structures.lexicon.global.InMemoryGlobalLexicon;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class MasterIndex extends Index{
	
	public MasterIndex(String path){
		super(path);
	}
	
	/*
	 * Lexicon Structures
	 */
	public GlobalLexicon getGlobalLexicon(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.glex"));
			//return new InMemoryGlobalLexicon(buffer, stats.getNumberOfUniqueTerms());
			return new GlobalLexicon(buffer, stats.getNumberOfUniqueTerms());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public GlobalLexicon getInMemoryGlobalLexicon(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.glex"));
			return new InMemoryGlobalLexicon(buffer, stats.getNumberOfUniqueTerms());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public GlobalLexiconInputStream getGlobalLexiconInputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.glex"));
			return new GlobalLexiconInputStream(buffer, stats.getNumberOfUniqueTerms());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public GlobalLexiconOutputStream getGlobalLexiconOutputStream(){
		try {
			BufferWrapper buffer = new FileWrapper(pool, getFileNumber(path+"/index.glex"));
			return new GlobalLexiconOutputStream(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
}
