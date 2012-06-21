package com.ntnu.laika.query.log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.ntnu.laika.query.Query;
import com.ntnu.laika.query.preprocessing.QueryPreprocessing;
import com.ntnu.laika.structures.Index;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class QueryLogFilter {
	
	public static void main(String args[]) throws IOException{
		Index index = new Index("/home/simonj/laikatest");
		
		BufferedReader in = new BufferedReader(new FileReader("/home/simonj/workstuff/Java/terrier_fork/querylog"));
		BufferedWriter out = new BufferedWriter(new FileWriter("/home/simonj/laikatest/querylog"));
		
		QueryPreprocessing preproc = new QueryPreprocessing(index.getLexicon(index.getStatistics().getNumberOfUniqueTerms()));
		
		String querystr;
		
		int min = Integer.MAX_VALUE, max = 0, sum = 0, cnt = 0, rmax = 0, rmin = Integer.MAX_VALUE, rsum = 0;
		int _min = Integer.MAX_VALUE, _max = 0, _sum = 0, _cnt = 0;
		int _rmin = Integer.MAX_VALUE, _rmax = 0, _rsum = 0;
		
		int curcnt, realcnt;
		while ( (querystr = in.readLine()) != null ){
			if (cnt % 5000 == 0) System.out.println(cnt);
			curcnt = querystr.split(" ").length;
			cnt++;
			sum+=curcnt;
			if (min > curcnt) min = curcnt;
			if (max < curcnt) max = curcnt;

			Query q = preproc.processQuery(querystr);
			realcnt = q!=null ? q.getNumberOfTerms() : 0;
			rsum+=realcnt;
			if (rmin > realcnt) rmin = realcnt;
			if (rmax < realcnt) rmax = realcnt;
			
			if (q==null || q.getNumberOfTerms() < 2) continue;
			
			out.write(querystr+"\n");

			_cnt++;
			_sum+=curcnt;
			if (_min > curcnt) _min = curcnt;
			if (_max < curcnt) _max = curcnt;
			
			realcnt = q.getNumberOfTerms();
			_rsum+=realcnt;
			if (_rmin > realcnt) _rmin = realcnt;
			if (_rmax < realcnt) _rmax = realcnt;
		}
		in.close();
		out.close();
		
		System.out.println("Done!");
		System.out.println("Old Stats:: count - " + cnt + ", min - " + min + ", max - " + max + ", avg - " + (double)sum/cnt);
		System.out.println("Real Old Stats:: min - " + rmin + ", max - " + rmax + ", avg - " + (double)rsum/cnt);
		System.out.println("New Stats:: count - " + _cnt + ", min - " + _min + ", max - " + _max + ", avg - " + (double)_sum/_cnt);
		System.out.println("Real New Stats:: min - " + _rmin + ", max - " + _rmax + ", avg - " + (double)_rsum/_cnt);
	}
}
