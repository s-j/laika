/*
 * Terrier - Terabyte Retriever 
 * Webpage: http://ir.dcs.gla.ac.uk/terrier 
 * Contact: terrier{a.}dcs.gla.ac.uk
 * University of Glasgow - Department of Computing Science
 * http://www.gla.ac.uk/
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is StopWords.java.
 *
 * The Original Code is Copyright (C) 2004-2009 the University of Glasgow.
 * All Rights Reserved.
 *
 * Contributor(s):
 *   Craig Macdonald <craigm{a.}dcs.gla.ac.uk> (original author)
 */

package com.ntnu.laika.query.preprocessing;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import gnu.trove.THashSet;

/** 
 * Implements stopword removal. Stopword list to load can be
 * passed in the constructor or loaded from the <tt>stopwords.filename</tt> property.
 * Note that it uses the system default encoding for the stopword list.
 * <b>Properties</b><br />
 * <ul><li><tt>stopwords.filename</tt> - the stopword list to load. More than one stopword list can be specified, by comma-separating
 * the filenames.</li>
 * <li><tt>stopwords.intern.terms</tt> - optimisation of Java for indexing: Stopwords terms are likely to appear extremely frequently
 * in a Collection, <a href="http://java.sun.com/j2se/1.5.0/docs/api/java/lang/String.html#intern()">interning</a> them in Java will
 * save on GC costs during indexing.</li>
 * <li><tt>stopwords.encoding</tt> - encoding of the file containing the stopwords, if not set defaults to <tt>trec.encoding</tt>,
 * and if that is not set, onto the default system encoding.</li></ul>
 * @author Craig Macdonald <craigm{a.}dcs.gla.ac.uk> 
 * @version $Revision: 1.24 $
 */
public class Stopwords 
{
	/** The hashset that contains all the stop words.*/
	protected final THashSet<String> stopWords = new THashSet<String>();

	/** Makes a new stopword term pipeline object. The stopwords file(s)
	  * are loaded from the filename parameter. If the filename is not absolute, it is assumed
	  * to be in TERRIER_SHARE. StopwordsFile is split on \s*,\s* if a comma is found in 
	  * StopwordsFile parameter.
	  * @param StopwordsFile The filename(s) of the file to use as the stopwords list. Split on comma,
	  * and passed to the (String[]) constructor.
	  */	
	public Stopwords(final String StopwordsFile)
	{
		if (StopwordsFile.indexOf(',') >= 0)
			loadStopwordsList(StopwordsFile.split("\\s*,\\s*"));
		else
			loadStopwordsList(StopwordsFile);
	}

	/** Makes a new stopword term pipeline object. The stopwords file(s)
	  * are loaded from the filenames array parameter. The non-existance of
	  * any file is not enough to stop the system. If a filename is  not absolute, it is
	  * is assumed to be in TERRIER_SHARE.
	  * @param StopwordsFiles Array of filenames of stopword lists.
	  * @since 1.1.0
	  */
	public Stopwords(final String StopwordsFiles[])
	{
		loadStopwordsList(StopwordsFiles);
	}

	/** Loads the specified stopwords files. Calls loadStopwordsList(String).
	  * @param StopwordsFiles Array of filenames of stopword lists.
	  * @since 1.1.0
	  */
	public void loadStopwordsList(final String StopwordsFiles[])
	{
		for(int i=0;i<StopwordsFiles.length;i++)
		{
			loadStopwordsList(StopwordsFiles[i]);
		}
	}

	/** Loads the specified stopwords file. Used internally by Stopwords(TermPipeline, String[]).
	  * @param stopwordsFilename The filename of the file to use as the stopwords list. */
	public void loadStopwordsList(String stopwordsFilename)
	{
		//determine encoding to use when reading the stopwords files
		
		//String stopwordsEncoding =  null;
		
		try {
			//use sys default encoding if none specified
			BufferedReader br = new BufferedReader(new FileReader(stopwordsFilename));
			String word;
			while ((word = br.readLine()) != null)
			{
				word = word.trim();
				if (word.length() > 0)
				{
					stopWords.add(word);
				}
			}
			br.close();
		} catch (IOException ioe) {
			System.err.println("Errror: Input/Output Exception while reading stopword list ("+stopwordsFilename+") :  Stack trace follows.");
			ioe.printStackTrace();
			
		}
		if (stopWords.size() == 0)
			System.err.println("Error: Empty stopwords file was used ("+stopwordsFilename+")");
	}


	/** Clear all stopwords from this stopword list object. 
	  * @since 1.1.0 */
	public void clear()
	{
		stopWords.clear();	
	}

	/** Returns true is term t is a stopword */
	public boolean isStopword(final String t)
	{
		return stopWords.contains(t);
	}

	
	/** 
	 * Checks to see if term t is a stopword. If so, return null.
	 */
	public final String processTerm(final String t)
	{
		return (stopWords.contains(t)) ? null :	t;
	}
}
