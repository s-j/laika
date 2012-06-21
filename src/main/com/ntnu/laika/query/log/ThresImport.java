package com.ntnu.laika.query.log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class ThresImport {
	public static void main(String[] args) throws Exception{
		Constants.USE_SKIPS = true;
		Constants.MAX_NUMBER_OF_RESULTS = 1;
		Index index = new Index(args[0]);
		Statistics stats = index.getStatistics();
		
		Lexicon lex = index.getLexicon(stats.getNumberOfUniqueTerms());
		
		//QueryProcessing proc = new AND_DAAT_MaxScore_QueryProcessing(index.getInvertedIndex(), stats);
		
		BufferedReader br = new BufferedReader(new FileReader("/home/simonjo/D1-Q2.thrs"));
		BufferedWriter wr = new BufferedWriter(new FileWriter("/home/simonjo/allpairs_scored"));
		
		String line, tmp[], terms[];
		int id1, id2;
		LexiconEntry lE;
		while ((line = br.readLine())!=null){
			tmp = line.split("\t");
			terms = tmp[0].split(" ");
			if (terms.length < 2) continue;
			
			lE = lex.lookup(terms[0]);
			if (lE == null) continue;
			id1 = lE.getTermId();
			
			lE = lex.lookup(terms[1]);
			if (lE == null) continue;
			id2 = lE.getTermId();
			
			wr.write(tmp[0] + "\t" + id1 + " " + id2 + "\t" + tmp[1]+"\n");
		}
		System.out.println("done!");
		wr.close();
		br.close();
		lex.close();
		index.close();
	}
}