package com.ntnu.laika.distributed.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;

import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.docdict.DocDict;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QualityChecker {
	private HashMap<String,HashSet<String>> answers = new HashMap<String, HashSet<String>>();
	private DocDict docDict;
	
	private double recall = 0.0d;
	private double MAP = 0.0d;
	private double P10 = 0.0d;
	private int cnt = 0;
	
	public QualityChecker(Index i){
		try {
			BufferedReader br = new BufferedReader(new FileReader("/mnt/data/data/laika_v2/index/qrels"));
			String tmp, tmps[];
			while ( (tmp=br.readLine()) != null) {
				tmps = tmp.split(" ");
				if (!tmps[3].equals("0")){
					tmp = (Integer.parseInt(tmps[0])-701)+"";
					HashSet<String> set = answers.get(tmp);
					if (set==null) {
						//System.out.print(" >"+tmp);
						set = new HashSet<String>();
						answers.put(tmp, set);
					}
					//System.out.println(tmp + " " + tmps[2]);
					set.add(tmps[2]);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		docDict = i.getDocDict(i.getStatistics().getNumberOfDocuments());
	}
	
	public void extraSort(int docids[], double scores[], int n){
	    int d; double s;
	    for (int pass=1; pass < n; pass++) {
	        for (int i=0; i < n-pass; i++) {
	            if (scores[i] < scores[i+1] || (scores[i] == scores[i+1] && docids[i] > docids[i+1])) {
	            	s = scores[i];
	            	scores[i] = scores[i+1]; 
	            	scores[i+1] = s;
	            	d = docids[i];
	            	docids[i] = docids[i+1];
	            	docids[i+1] = d;
	            }
	        }
	    }
	}
	
	public void checkResults(String qid, QueryResults results){
		int[] docids = results.getDocids();
		double[] scores = results.getScores();
		int rescnt = docids.length;
		checkResults(qid, rescnt, docids, scores);
	}
	
	public void checkResults(String qid, int rescnt, int[] docids, double[] scores){
		if (!answers.containsKey(qid)) return;
		int corrno = 0;
		double MAP = 0.0d;
		double P10 = 0.0;
		
		String docno;
		extraSort(docids, scores, rescnt);

		for (int i=0; i < rescnt; i++) {
			if (scores[i]>0.0d){
				docno = docDict.lookup(docids[i]).getDocno();
				if (answers.get(qid).contains(docno)){
					corrno++;
					MAP += ((double)corrno)/(i+1);
				}
			}
			if (i==9){
				P10 = ((double)(corrno))/10;
			}
		}
		if (rescnt < 10) P10 = ((double)(corrno))/rescnt;
		int basecnt = answers.get(qid).size();
		 
		MAP /= basecnt;
		double recall = ((double)corrno)/basecnt;
		System.out.println(qid+ "\t" + rescnt+"/"+corrno+"/"+basecnt + "\t" + df6.format(MAP) + "\t" + P10 + "\t" + df6.format(recall));
		
		this.MAP += MAP;
		this.P10 += P10;
		this.recall += recall;
		this.cnt++;
	}
	
	private DecimalFormat df6 = new DecimalFormat("#0.000000");
	
	public void printResults(){
		System.out.println("#MAP: " + df6.format(MAP/cnt) + " P10: " + df6.format(P10/cnt) + " Rec: " + df6.format(recall/cnt));
	}
}
