package com.ntnu.laika.distributed.dp;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class DPSubQuery {
	private int qId;
	private int numterms;
	private int[] terms;
	private int[] nts;
	private int[] tfs;
	private int[] qfs;
	private double[] maxScores;
	private int maxFreq = 0;
	private long processingTime = 0;
	
	public DPSubQuery(ChannelBuffer buffer, int myid){
		qId = buffer.readInt();
		numterms = buffer.readByte();
		maxFreq = buffer.readByte();
		terms = new int[numterms];
		nts = new int[numterms];
		tfs = new int[numterms];
		qfs = new int[numterms];
		maxScores = new double[numterms];
		
		for (int j=0; j<numterms; j++){
			terms[j]=buffer.readInt();
			nts[j]=buffer.readInt();
			tfs[j]=buffer.readInt();
			qfs[j]=buffer.readByte();
			maxScores[j]=buffer.readDouble();
		}
	}
		
	public final int getNumTerms(){
		return numterms;
	}
	
	public final int getTermID(int i){
		return terms[i];
	}
	
	public final int getNT(int i){
		return nts[i];
	}
	
	public final int getTF(int i){
		return tfs[i];
	}
	
	public final int getQF(int i){
		return qfs[i];
	}
	
	public final double getMaxScore(int i){
		return maxScores[i];
	}
	
	public final double[] getMaxScores(){
		return maxScores.clone();
	}
	
	public final int getQueryID(){
		return qId;
	}
	
	public final int getMaximumKeyFrequency(){
		return maxFreq;
	}

	public void incrementProcessingTime(long time) {
		processingTime += time;
	}
	
	public long getProcessingTime() {
		return processingTime;
	}
}
