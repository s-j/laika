package com.ntnu.laika.fromterrier;

import java.io.IOException;

import uk.ac.gla.terrier.structures.DocumentIndexInputStream;
import uk.ac.gla.terrier.structures.Index;
import uk.ac.gla.terrier.structures.LexiconInputStream;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TerrierIndexChecker {
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/home/simonj/workstuff/Java/terrier";
		@SuppressWarnings("unused")
		int id = 1321126;
		if (args.length>0){
			inpath = args[0];
			id = Integer.parseInt(args[1]);
		}
		System.setProperty("terrier.home", inpath);
		System.setProperty("terrier.etc",  inpath + "/etc");
		System.setProperty("terrier.setup", inpath + "/etc/terrier.properties");
		
		uk.ac.gla.terrier.structures.Index tIndex = Index.createIndex();
		/*
		LexiconInputStream lis = (LexiconInputStream)tIndex.getIndexStructureInputStream("lexicon");
		InvertedIndexInputStream iis = (InvertedIndexInputStream) tIndex.getIndexStructureInputStream("inverted");
		int j=0;
		while (lis.readNextEntry() > -1){
			int[][] scores = iis.getNextDocuments();
			for (int i=0; i<scores[0].length; i++){
				if (scores[0][i] > 2520926) System.out.println(lis.getTermId() + " -> " + scores[0][i]);
			}
			if (j++ == id){
				System.out.println(lis.getTermId()+" "+lis.getTerm()+" "+lis.getNt()+" "+lis.getTF());
				for (int i=0; i<scores[0].length; i++){
					System.out.println(scores[0][i]+" "+scores[1][i]);
				}
				System.exit(0);
			}
		}
		*/
		
		LexiconInputStream lis = (LexiconInputStream)tIndex.getIndexStructureInputStream("lexicon");

		int maxlenght = 0, blen;
		while (lis.readNextEntry() > -1){
			blen = lis.getTerm().getBytes().length;
			maxlenght = maxlenght >= blen ? maxlenght : blen;
		}
		lis.close();
		System.out.println("TermMaxLength: " + maxlenght);
		maxlenght = 0;
		DocumentIndexInputStream dis = new DocumentIndexInputStream();
		while (dis.readNextEntry() > -1){
			blen = dis.getDocumentNumber().getBytes().length;
			maxlenght = maxlenght >= blen ? maxlenght : blen;	
		}
		dis.close();
		System.out.println("DocNoMaxLength: " + maxlenght);
		tIndex.close();
	}
}
