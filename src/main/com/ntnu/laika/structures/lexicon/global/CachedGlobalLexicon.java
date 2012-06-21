package com.ntnu.laika.structures.lexicon.global;

import java.io.IOException;
import java.util.HashMap;
import com.ntnu.laika.structures.BufferWrapper;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class CachedGlobalLexicon extends GlobalLexicon{
	
    public CachedGlobalLexicon(BufferWrapper buffer, int entries) throws IOException, InterruptedException {
       super(buffer, entries);
    }
    
    private HashMap<String, GlobalLexiconEntry> termCache = new HashMap<String, GlobalLexiconEntry>(1000);
    
    public GlobalLexiconEntry lookup(String term)  {
    	GlobalLexiconEntry ret = termCache.get(term);
    	if (ret != null ) return ret;
    	
    	ret = super.lookup(term);
    	if (ret != null ) termCache.put(term, ret);
    	return ret;
    }
    
    private HashMap<Integer, GlobalLexiconEntry> idCache = new HashMap<Integer, GlobalLexiconEntry>(1000);
    
    public GlobalLexiconEntry lookup(int termId) {
    	GlobalLexiconEntry ret = idCache.get(termId);
    	if (ret != null ) return ret;
    	
    	ret = super.lookup(termId);
    	if (ret != null ) idCache.put(termId, ret);
    	return ret;    	
    }
}