package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.preprocessing.PorterStemmer;
import com.ntnu.laika.query.preprocessing.Stopwords;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.utils.Closeable;

/**
 * NOTE: The method is not thread-safe!
 * 
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QueryLogFilter implements Closeable{
	protected Lexicon lexicon;
	
	protected PorterStemmer stemmer;
	protected Stopwords stopwords;
	
	
	public QueryLogFilter(Lexicon lexicon2){
		this.lexicon = lexicon2;
		stopwords = new Stopwords("/mnt/data/data/laika_v2/index/stopword-list.txt");
		stemmer = new PorterStemmer(); 
	}
	
	protected String removeNonAlphaNumChars(String t) {
		final int t_length = t.length();
		StringBuilder buffer = new StringBuilder(t_length);
		char c;
		for (int i=0; i<t_length; i++) {
			if (Character.isLetterOrDigit(c = t.charAt(i)))
				buffer.append(c);
		}
		return buffer.toString();
	}
			
		
	public String processQuery(String querystr){
		int numt=0;
		
		String[] terms = new String[Constants.MAX_QUERY_LENGTH*2];
		
		//to lower case
		querystr = querystr.toLowerCase();
		String term;
		for (String token : querystr.split(" ")){
			//remove nonalphanumerics
			//nonalpharemove -> stopwordremove -> stemming -> queryproc
			term=stemmer.processTerm(
							stopwords.processTerm(
								removeNonAlphaNumChars(token)));
			if (term!=null) terms[numt++]=term;
		}
		
		if (numt == 0) return null;
		
		Arrays.sort(terms, 0, numt);
		StringBuffer out = new StringBuffer();
		
		for ( int i=0; i<numt; i++){
			term = terms[i];
			//if (lexicon.lookup(term) != null) {
				if (out.length() > 0) out.append(' ');
				out.append(term);
			//}
		}
		return out.toString();
	}

	@Override
	public void close() {
		lexicon.close();
	}
	
	
	
	public static void main(String args[]) throws IOException{
		Index index = new Index("/mnt/data/data/laika_v2/index");
		QueryLogFilter filter = new QueryLogFilter(index.getLexicon(index.getStatistics().getNumberOfUniqueTerms()));
		HashSet<String> cache = new HashSet<String>();
//		BufferedReader reader = new BufferedReader(new FileReader(index.getPath()+"/effq"));
		BufferedReader reader = new BufferedReader(new FileReader("/home/simonj/querylog_old"));
//		BufferedWriter writer = new BufferedWriter(new FileWriter(index.getPath()+"/effq_cleaned"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/simonj/querylog_cleaned_old2"));
		String query;//, tmp[];
		int i=0;
		while ((query = reader.readLine()) != null){
			//tmp = query.split(":");
			//System.out.println(tmp[1]);
			query = filter.processQuery(query);
			if (query != null && query.length() > 0){
				if (cache.add(query))
					writer.write(query+"\n");
			}
			//query = filter.processQuery(query);
			//if (query!=null && query.length() > 0) writer.write(query+"\n");
			if (++i % 10000 == 0) System.out.println(i);
			
		}
		System.out.println("i");
		reader.close();
		writer.close();
		index.close();
	}
}