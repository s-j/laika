package com.ntnu.lpsolver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import gnu.trove.TLongDoubleHashMap;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class AccScorePredictor {
	private static TLongDoubleHashMap map;
	
	public static void loadData(String path){
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			map = new TLongDoubleHashMap();
			return;
		}
		map = new TLongDoubleHashMap(210000);
		String line, tmp[], ids[];
		int t1, t2; double ms;
		try {
			while ( (line = br.readLine()) != null) {
				tmp = line.split("\t");
				ids = tmp[1].split(" ");
				t1 = Integer.parseInt(ids[0]);
				t2 = Integer.parseInt(ids[1]);
				ms = Double.parseDouble(tmp[2]);
				map.put((long) t1 << 32 | t2, ms);	
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("loaded pairs");
	}
	
	public static double getEntry(int t1, int t2){
		long key = (long) t1 << 32 | t2;
		if (map.containsKey(key)){
			return map.get(key);
		} else { 
			key = (long) t2 << 32 | t1;
			if (map.containsKey(key)){
				return map.get(key);
			} else { 
				return -1.0d;
			}
		}
	}
	
	public static void main(String []args){
		
		map = new TLongDoubleHashMap(210000);
		
		int[] ids = {42, 13, 49};
		double maxScores[] = {6.0, 5.6, 2.3};
		
		map.put((long) 42 << 32 | 13, 3.2);
		map.put((long) 49 << 32 | 42, 6.1);
		map.put((long) 49 << 32 | 13, 6.0);
		/*
		map.put((long) 2 << 32 |3, 5.3630040326928325);
		map.put((long) 2 << 32 |4, 3.7692956768035986);
		map.put((long) 1 << 32 |2, 9.505736691544072);
		map.put((long) 1 << 32 |3, 8.299026861772711);
		map.put((long) 0 << 32 |1, 13.699385150163463);
		map.put((long) 0 << 32 |2, 10.831916805363697);
		map.put((long) 0 << 32 |3, 9.622883909871316);
		map.put((long) 0 << 32 |4, 8.017670734527595);
		*/
		
		double[] res = getANDAccScores(ids, maxScores, 3);
		System.out.println(res[0]);	
		System.out.println(res[1]);
		System.out.println(res[2]);
		/*
		System.out.println(res[3]);
		System.out.println(res[4]);
		*/
		double[] res2 = getAccScoresORTable(ids, maxScores);
		System.out.println(res2[res2.length-1]);
		//System.out.println(res2[getORAccScoresIndex(ids, new int[]{1,3}, 2)]);
		/*
		int[] IDs = {1,2,3,4};
		int[] IDq = {3,4,2};
		System.out.println(getORAccScoresIndex(IDs, IDq, IDq.length));
		*/
	}
	
	public static void fillAccScoresAND(double accScores[], double[] maxScores, int ids[], int numTerms){
		int numTermsMinusOne = numTerms-1;
		int IDs[] = new int[numTerms];
		double ms[] = new double[numTerms];
		for (int i=0; i<numTerms; i++){
			IDs[numTermsMinusOne-i] = ids[i];
			ms[numTermsMinusOne-i] = maxScores[i];
		}
		
		double _accScores[] = AccScorePredictor.getANDAccScores(IDs, ms, numTerms);
		for (int i=0; i<numTerms; i++){
			accScores[numTermsMinusOne-i] = _accScores[i];
		}
		//accScores[0] = accScores[1] + maxScores[0];
	/*
		int numTermsMinusOne = numTerms-1;
		int IDs[] = new int[numTermsMinusOne];
		double ms[] = new double[numTermsMinusOne];
		for (int i=1; i<numTerms; i++){
			IDs[numTermsMinusOne-i] = ids[i];
			ms[numTermsMinusOne-i] = maxScores[i];
		}
		
		double _accScores[] = AccScorePredictor.getANDAccScores(IDs, ms, numTermsMinusOne);
		for (int i=0; i<numTermsMinusOne; i++){
			accScores[numTermsMinusOne-i] = _accScores[i];
		}
		accScores[0] = accScores[1] + maxScores[0];
	*/
	}
		
	/**
	 * Gives an index of the subsequence in the precomputed array.
	 * @param OrigIDs the original ID array
	 * @param IDs the subsequence array
	 * @param n number of elements, only entries 0..n-1 from IDs will be used 
	 * @return index of the original subsequence
	 */
	public static int getAccScoresORIndex(int OrigIDs[], int IDs[], int offset, int endoffset){
		int key = 0, e;
		for (int i=offset; i<endoffset; i++){
			e = 1;
			for (int j=0; j<OrigIDs.length; j++){
				if (IDs[i] != OrigIDs[j]) e<<=1;
				else break;
			}
			key |= e;
		}
		return key;
	}

	public static double[] getAccScoresORTable(int IDs[], double[] maxScores){
		int n = IDs.length;
		int np = 1<<n;

		LPSolver lp = new LPSolver();
		Edge[] edgebuffer = new Edge[n*(n-1)>>1];

		int[] tmpIDs = new int[n];
		double[] tmpMaxScores = new double[n];
		double[] out = new double[np];
		double score;
		for (int i=1; i<np; i++){
			int bitv = i;
			int pos = 0;
			int opos = 0;
			while (bitv>0){
				if ((bitv & 1) == 1){
					tmpIDs[opos]=IDs[pos];
					tmpMaxScores[opos]=maxScores[pos];
					opos++;
				}
				bitv >>= 1;
				pos++;
			}
			score = getAccScore(tmpIDs, tmpMaxScores, opos, lp, edgebuffer);
			for (int j=i; j<np; j++) {
				if ((i & j) == i && out[j] < score) out[j] = score;
			}
		}
		return out;
	}
	
	//NOTE: this is reverse!
	public static double getAccScore(int IDs[], double[] maxScores, int num, LPSolver lp, Edge[] edgebuffer){
		if (num == 1) {
			return maxScores[0];
		} else if (num == 2) {
			double ps = getEntry(IDs[0],IDs[1]);
			return (ps >= 0.0d) ? ps : maxScores[0] + maxScores[1];
		} else {
			int numE = 0;
			double ps;
			for (int i = 0; i < num; i++){						//store other edges...
				for (int j = i + 1; j < num; j++){					
					if ((ps = getEntry( IDs[i], IDs[j])) >= 0.0d){
						edgebuffer[numE++] = new Edge(i, j, ps);
					}
				}
			}
			if (numE > 0){
				return lp.calculateThreshold(num, numE, maxScores, edgebuffer);
			} else {
				double accScore = 0.0d;
				for (int i=0; i<num; i++) accScore += maxScores[i];
				return accScore;
			}
		}	
	}
	
	
	public static double[] getANDAccScores(int IDs[], double[] maxScores, int n){
		double accScores[] = new double[n];
		if (n == 0) return accScores;
		
		accScores[0] = maxScores[0];
		if (n == 1) return accScores;

		double ps = getEntry(IDs[0],IDs[1]);
		accScores[1] = ps < 0.0d ? maxScores[0] + maxScores[1] : ps;
		
		if (n == 2) return accScores;
		
		Edge[] edges = new Edge[n*(n-1)>>1];
		int numE = 0;
		if (ps >= 0.0d) edges[numE++] = new Edge(0, 1, ps);	//store the edge from n-2 to n-1...
		
		LPSolver lp = new LPSolver();							
		for (int j = 2; j < n; j++){						//store other edges...
			for (int i = 0; i < j; i++){					
				if ((ps = getEntry( IDs[i], IDs[j])) >= 0.0d){
					//if the edge exists...
					edges[numE++] = new Edge(i, j, ps);
				}
			}
			//accScores[j] = (newE > 0 & (j + 1) <= MAX_QUERY_TERM_COUNT) ?
			//		lp.calculateThreshold(j+1, numE, maxScores, edges) : (accScores[j - 1] + maxScores[j]);
			accScores[j] = lp.calculateThreshold(j+1, numE, maxScores, edges);
		}
		
		return accScores;
	}
}