package com.ntnu.laika.structures.fastmaxscore;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FastMaxScore {
	private static boolean loaded = false;
	private static double[] maxScores;
	
	public static void setMaxScores(int totEntries, int readEntries, FastMaxScoreInputStream inputStream){
		maxScores = new double[totEntries];
		for (int i=0; i<readEntries; i++){
			inputStream.nextEntry();
			maxScores[inputStream.curId] = inputStream.curMaxScore; 
		}
		inputStream.close();
		loaded = true;
	}
	
	//unadjusted
	public static final double getMaxScore(int termid){
		return loaded ? maxScores[termid] : Double.NaN;
	}
	
	private static double k_3 = 8d;
	
	public static final double getMaxScore(int termid, double keyFrequency){
		return maxScores[termid]*((k_3+1d)*keyFrequency/(k_3+keyFrequency));
	}
}