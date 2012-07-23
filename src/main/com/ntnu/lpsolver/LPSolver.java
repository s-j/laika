package com.ntnu.lpsolver;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Emre Varol & Enver Kayaaslan
 */
public class LPSolver {	
	private Edge []edges;
	private int N, E;
	private int[] state, use, last;
	private double min, runningmin;
	private double [] mins;
	public static int MAX_QUERY_TERM_COUNT = 8;
	
	double calculateThreshold(int numTerms, int numEdges, double[] maxScores, Edge[] txtScores) {
		if (numTerms > MAX_QUERY_TERM_COUNT) {
			double score = 0.0d;
			for (double s : maxScores) score += s;
			return score;
		}
		int i;
		N = numTerms;
		E = numEdges;
		edges = new Edge[E+N];
		for (i = 0; i < N; i++){
			edges[i]= new Edge(i, i, maxScores[i]);
		}
		
		E += N;
		Edge _e;
		for (i = N; i < E; i++){
			_e = txtScores[i-N];
			edges[i] = new Edge(_e.x, _e.y, _e.val);
		}
	
		state = new int[N];
		use = new int[E];
		mins = new double[N];
		last = new int[N];
		min = 0;
		runningmin = 0;
		
		for (i = 0; i<N; mins[i++] = -1);
		for (i = 0; i<N; last[i++] = -1);
		for (i = 0; i<N; state[i++] = 0);
		for (i = 0; i<E; use[i++] = 0);
		
		Arrays.sort(edges, Edge.ORDER);

		for (i = 0; i < E; i++) {
 			if (state[edges[i].x] == 0 && state[edges[i].y] == 0) {
				min += edges[i].val;
				state[edges[i].x] = state[edges[i].y] = 2;				
			}
			double val = (edges[i].x == edges[i].y)? edges[i].val: edges[i].val/2;
			if (mins[edges[i].x] == -1 || mins[edges[i].x] > val)
				mins[edges[i].x] = val;
			if (mins[edges[i].y] == -1 || mins[edges[i].y] > val)
				mins[edges[i].y] = val;
			last[edges[i].x] = last[edges[i].y] = i;
		}
		for (i = 0; i < N; i++) {
			runningmin += mins[i];
			state[i] = 0;
		}
		//System.out.println("Min: " + runningmin + " - " + min);
		searchrec(0);
		//System.out.println("Min: " + min);
		//System.out.println("Count: " + count);
		return min;
	}
	
	public void searchrec(int k) {
		if (runningmin >= min)
			return;
		if (k == E) {
			min = runningmin;
			return;
		}
		int x = edges[k].x;
		int y = edges[k].y;
		double val = edges[k].val;
		if (x == y) {
			if (last[x] == k) {
				if (state[x] < 2) {
					runningmin -= mins[x]*(2-state[x])/2;
					state[x] += 2;
					runningmin += val;
					use[k] = 2;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val;
					state[x] -= 2;	
					runningmin += mins[x]*(2-state[x])/2;
				}
				if (state[x] >= 2) 
					searchrec(k+1);	
			}
			else {
				if (state[x] < 2) {
					runningmin -= mins[x]*(2-state[x])/2;
					state[x] += 2;
					runningmin += val;
					use[k] = 2;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val;
					state[x] -= 2;
					runningmin += mins[x]*(2-state[x])/2;
				}
				searchrec(k+1);
			}
		}
		else {
			if (last[x] == k || last[y] == k) {
				if (state[x] == 0 || state[y] == 0) {
					double runningval = 0;
					if (state[x] < 2)
						runningval += mins[x]*(2-state[x])/2;
					if (state[y]<2)
						runningval += mins[y]*(2-state[y])/2;
					runningmin -= runningval;
					state[x] += 2;
					state[y] += 2;			
					runningmin += val;
					use[k] = 2;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val;
					state[x] -= 2;
					state[y] -= 2;
					runningmin += runningval;
				}
				if ((state[x] != 0 || last[x] != k) && (state[y] !=0 || last[y] != k) && (state[x] < 2 || state[y] < 2)) {
					double runningval = 0;
					if (state[x] < 2)
						runningval += mins[x]/2;
					if (state[y] < 2)
						runningval += mins[y]/2;
					runningmin -= runningval;
					state[x] += 1;
					state[y] += 1;			
					runningmin += val/2;
					use[k] = 1;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val/2;
					state[x] -= 1;
					state[y] -= 1;
					runningmin += runningval;
				}
				if ((state[x] >= 2 || last[x] != k) && (state[y] >= 2 || last[y] != k))
					searchrec(k+1);
			}
			else {
				if (state[x] == 0 || state[y] == 0) {
					double runningval = 0;
					if(state[x] < 2)
						runningval += mins[x]*(2-state[x])/2;
					if(state[y] < 2)
						runningval += mins[y]*(2-state[y])/2;
					runningmin -= runningval;
					state[x] += 2;
					state[y] += 2;
					runningmin += val;
					use[k] = 2;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val;
					state[x] -= 2;
					state[y] -= 2;
					runningmin += runningval;
				}
				if (state[x] < 2 || state[y] < 2) {
					double runningval = 0;
					if (state[x] < 2)
						runningval += mins[x]/2;
					if (state[y] < 2)
						runningval += mins[y]/2;
					runningmin -= runningval;
					state[x] += 1;
					state[y] += 1;			
					runningmin += val/2;
					use[k] = 1;
					searchrec(k+1);
					use[k] = 0;
					runningmin -= val/2;
					state[x] -= 1;
					state[y] -= 1;
					runningmin += runningval;
				}
				searchrec(k+1);
			}
		}
	}
	
	public static long getKey(int x, int y){
		return (long) x << 32 | y;
	}
	
	public static int[] splitKey(long key){
		return new int[]{(int)(key >>> 32),(int)(key & 4294967295l)};
	}
	
	public static void main(String [] args) throws IOException {
		//TODO: if any term or pair 0 -> 0!
		double maxScores[] = {0.7, 0.6, 0.5};
		int n = 3;
		Edge[] edges = new Edge[n*(n-1)>>1];
		edges[0] = new Edge(0, 1, 1.0);
		edges[1] = new Edge(1, 2, 0.6);
		LPSolver lp = new LPSolver();
		double res = lp.calculateThreshold(maxScores.length, 2 ,maxScores, edges);
		System.out.println(res);
		
		/*		double maxScores[] = {7.644408230007376, 6.323751317553276, 3.380148562453018, 2.0741925935027514, 0.42728409565352876};
		int n = 5;
		Edge[] edges = new Edge[n*(n-1)>>1];
		edges[0] = new Edge(3, 4, 2.4782522099296305);
		edges[1] = new Edge(2, 3, 5.3630040326928325);
		edges[2] = new Edge(2, 4, 3.7692956768035986);
		edges[3] = new Edge(1, 2, 9.505736691544072);
		edges[4] = new Edge(1, 3, 8.299026861772711);
		edges[5] = new Edge(0, 1, 13.699385150163463);
		edges[6] = new Edge(0, 2, 10.831916805363697);
		edges[7] = new Edge(0, 3, 9.622883909871316);
		edges[8] = new Edge(0, 4, 8.017670734527595);
		LPSolver lp = new LPSolver();
		double res = lp.calculateThreshold(maxScores.length, 9 ,maxScores, edges);
		System.out.println(res);
		*/
	}
}