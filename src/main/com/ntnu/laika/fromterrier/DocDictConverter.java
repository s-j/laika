package com.ntnu.laika.fromterrier;

import java.io.IOException;

import com.ntnu.laika.structures.docdict.DocDictEntry;
import com.ntnu.laika.structures.docdict.DocDictInputStream;
import com.ntnu.laika.structures.docdict.DocDictOutputStream;
import com.ntnu.laika.structures.docdict.ShortDocDictOutputStream;

import uk.ac.gla.terrier.structures.DocumentIndexInputStream;
import uk.ac.gla.terrier.structures.Index;
import uk.ac.gla.terrier.utility.ApplicationSetup;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DocDictConverter {
	
	public static void convertDocDict(String terrierpath, String dstpath){
		System.setProperty("terrier.home", terrierpath);
		System.setProperty("terrier.etc", terrierpath + "/etc/");
		System.setProperty("terrier.setup", terrierpath + "/etc/terrier.properties");
		
		uk.ac.gla.terrier.structures.Index tIndex = Index.createIndex();
		DocumentIndexInputStream docstream = new DocumentIndexInputStream(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX);

		
		com.ntnu.laika.structures.Index lIndex = new com.ntnu.laika.structures.Index(dstpath);
		DocDictOutputStream ddow = lIndex.getDocDictOutputStream();
		
		int i=0; 
		try {
			while (docstream.readNextEntry() != -1){
				//System.out.println(docstream.getDocumentId());
				ddow.nextEntry(
				docstream.getDocumentId(),
				docstream.getDocumentNumber(),
				docstream.getDocumentLength());
				if (++i % 5000000 == 0) System.out.println(i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		docstream.close();
		tIndex.close();
		ddow.close();
		lIndex.close();
	}
	
	public static void docDict2Short(String src, String dst){	
		com.ntnu.laika.structures.Index sIndex = new com.ntnu.laika.structures.Index(src);
		int numdocs = sIndex.getStatistics().getNumberOfDocuments();
		DocDictInputStream dis = sIndex.getDocDictInputStream();
		
		com.ntnu.laika.structures.Index dIndex = new com.ntnu.laika.structures.Index(dst);
		ShortDocDictOutputStream sdos = dIndex.getShortDocDictOutputStream();
		
		System.out.println(numdocs);
		for (int i=0; i<numdocs; i++){
			DocDictEntry entry = dis.nextEntry();
			sdos.nextEntry(entry.getDocid(), entry.getNumberOfTokens());
			if (i % 5000000 == 0) System.out.println(i);
		}
		
		dis.close();
		sIndex.close();
		sdos.close();
		dIndex.close();	
	}
	
	
	public static void main(String args[]) throws IOException, InterruptedException{
		String inpath = "/home/simonj/workstuff/Java/terrier";
		String outpath ="/home/simonj/terrier";
		if (args.length>0){
			inpath = args[0];
			outpath= args[1];
		}
		convertDocDict(inpath,outpath);
		//docDict2Short(outpath, outpath2);
		//new DocDictConverter("/home/simonj/workstuff/Java/terrier/","/home/simonj/laikatest/");
		//docDict2Short("/home/simonj/data/laikatest/", "/home/simonj/data/laikatest_dist/1/");
	}
}