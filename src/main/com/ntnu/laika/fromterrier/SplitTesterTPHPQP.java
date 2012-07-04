package com.ntnu.laika.fromterrier;

import java.io.IOException;

import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconEntry;
import com.ntnu.laika.structures.lexicon.global.GlobalLexiconInputStream;
import com.ntnu.laika.utils.BitVector;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class SplitTesterTPHPQP {

	public SplitTesterTPHPQP(String lexmap, String mainpath, int numnodes) throws IOException, InterruptedException{
		TPLexMapping map = new TPLexMapping(lexmap, numnodes);
		System.out.println("map is loaded...");
		MasterIndex input_index = new MasterIndex(mainpath);
		Statistics stats = input_index.getStatistics();
		int numterms = stats.getNumberOfUniqueTerms();
		
		GlobalLexiconInputStream glis = input_index.getGlobalLexiconInputStream();
	
		for (int i=0; i<numterms; i++){
			GlobalLexiconEntry _lE = glis.nextEntry();
			String term = _lE.getTerm();
			BitVector bv = map.getBitVector(term);
			if (bv != null) {
				byte[] isign = _lE.getSignature();
				byte[] osign = bv.data;
				for (int j=0; j<isign.length; j++){
					if (isign[j]!=osign[j]) System.out.println("wrong!");
				}
			}
			if (i % 1000000 == 0) System.out.println((100 * i)/numterms + "%");
		}
		glis.close();		
		input_index.close();
	}
	
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String mapping = "/home/simonj/workstuff/2012/Enver/partvec_term/partvec_term.8.HP.CON.RYES";
		String idxdst = "/mnt/data/data/ENVERIDX/8.HP.CON.RYES/0/";
		int numnodes = 8;
		
		System.out.println(mapping);
		new SplitTesterTPHPQP(mapping, idxdst, numnodes);
		
		System.out.println("Done! =)");
	}
}
