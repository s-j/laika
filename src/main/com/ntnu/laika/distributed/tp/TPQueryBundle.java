package com.ntnu.laika.distributed.tp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.ntnu.laika.Constants;
import com.ntnu.laika.distributed.util.AccumulatorSet;
import com.ntnu.laika.distributed.util.AccumulatorSetCompression;
import com.ntnu.laika.distributed.util.LinkedByteBuffer;
import com.ntnu.network.ApplicationHandler;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class TPQueryBundle {
	private ChannelBuffer originalquery;
	
	private int numterms;
	private int[] terms;
	private int[] freqs;
	private double[] maxScores;
	
	private int maxFreq = 0;
	
	private double lowVal = Double.MIN_VALUE;
	private double highVal = Double.MAX_VALUE;
	private double kThBest = 0.0d;
	
	private int nextnodeid = 0;
	private int numsubs;
	public int numsubsleft;
	private int qId;
	
	private double remScore = 0.0d;
	
	private long processingTime = 0l;
	
	private AccumulatorSet accset = null;
	
	public TPQueryBundle(ChannelBuffer buffer, int myid){
	//	System.out.println("parsing query bundle!");
		int startpos = buffer.readerIndex();
		
		qId = buffer.readInt();
		maxFreq = buffer.readByte();
		numsubs = buffer.readByte();
		
		//System.out.println("---" + qId + " : " + numsubs);
		int nodeid = 0, nterms = 0;
		int _state = 0; //0 - before myid, 1 - nextone, 2 - rest.  
		for (int i=0; i<numsubs; i++){
			nodeid = buffer.readByte(); //nodeid
			nterms = buffer.readByte(); //nterms
			//System.out.println(">> " + nodeid + " " +nterms);
			if (nodeid != myid) {
				switch (_state) {
					case 0:
						buffer.skipBytes(nterms*(Constants.INT_SIZE + 1 + Constants.DOUBLE_SIZE));		
						break;
					case 1: 
						nextnodeid = nodeid;
						_state = 2;
					case 2:
						for (int j=0; j<nterms; j++){
							buffer.skipBytes(Constants.INT_SIZE + 1);
							remScore += buffer.readDouble();
						}
				}
			} else {					//hey, its me! -> read my data
				numsubsleft = numsubs - i - 1;
				numterms = nterms;	
				terms = new int[nterms];
				freqs = new int[nterms];
				maxScores = new double[nterms];
				for (int j=0; j<nterms; j++){
					terms[j]=buffer.readInt();
					freqs[j]=buffer.readByte();
					maxScores[j]=buffer.readDouble();
					//System.out.println(terms[j] + " " + freqs[j] + " " + maxScores[j]);
				}
				_state = 1;
				//System.out.println(myid + " > " + numsubsleft + "(" + numsubs + ")");
			}
		}
		
		//done with query data, copy the original buffer in case we need it later 
		int endpos = buffer.readerIndex();
		originalquery = buffer.slice(startpos, endpos-startpos);
		
		if (buffer.readableBytes() > 0){
			accset = new AccumulatorSet();
			lowVal = buffer.readDouble();
			highVal = buffer.readDouble();
			kThBest = buffer.readDouble();
			int cnt = buffer.readInt();
			byte compression = buffer.readByte();
			AccumulatorSetCompression.fromByteBuffer(buffer, accset, cnt, compression);
			processingTime = buffer.readLong();
		}
	}
		
	public final AccumulatorSet getAccumulatorSet(){
		return accset;
	}
	
	public final void setAccumulatorSet(AccumulatorSet accset){
		this.accset = accset;
	}
	
	public final int getNumTerms(){
		return numterms;
	}
	
	public final int getTermID(int i){
		return terms[i];
	}
	
	public final int getTermQF(int i){
		return freqs[i];
	}
	
	public final double getMaxScore(int i){
		return maxScores[i];
	}
	
	public final double[] getMaxScores(){
		return maxScores.clone();
	}
	
	
	public final double getRemScore(){
		return remScore;
	}
	
	public final int getNextNodeID(){
		return nextnodeid;
	}
	
	public final int getNumSubQueries(){
		return numsubs;
	}
	
	public final double getLowVal(){
		return lowVal;
	}

	public final double getHighVal(){
		return highVal;
	}
	
	public final int getQueryID(){
		return qId;
	}
	
	public final int getMaximumKeyFrequency(){
		return maxFreq;
	}
	
	public long getProcessingTime(){
		return processingTime;
	}
	
	public void incrementProcessingTime(long newtime){
		processingTime += newtime;
	}
	
	public final ChannelBuffer packNextQueryBundle(AccumulatorSet accs, double newlow, double newhigh, double newKthBest){
		int cnt = accs.size();
		byte compression = AccumulatorSetCompression.DOCIDSONLY;
		LinkedByteBuffer bbuffer = AccumulatorSetCompression.toByteBuffer(accs, compression);
		
		ChannelBuffer out = ChannelBuffers.buffer(bbuffer.getSize() + originalquery.readableBytes() + Constants.INT_SIZE + Constants.DOUBLE_SIZE*3 + 2 + Constants.LONG_SIZE);
		
		bbuffer.flip();
		out.writeByte(ApplicationHandler.QUERY_BUNDLE);
		out.writeBytes(originalquery);
		originalquery.resetReaderIndex();
		out.writeDouble(newlow);
		out.writeDouble(newhigh);
		out.writeDouble(newKthBest);
		out.writeInt(cnt);
		
	//	System.out.println(">-->"+cnt+ "("+out.writerIndex()+")");
		out.writeByte(compression);
		//out.writeBytes(bbuffer);
		//ByteBufferFactory.freeByteBuffer(bbuffer);
		bbuffer.flushToChannelBufferAndFree(out);
		out.writeLong(processingTime);
		return out;
	}

	public double getKthBest() {
		return kThBest;
	}
}
