package com.ntnu.laika.structures.fastmaxscore;

import com.ntnu.laika.structures.BufferWrapper;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class FastMaxScoreInputStream implements Closeable{
	private BufferWrapper buffer;
	
	private int numberOfTerms = 0;
	
    public FastMaxScoreInputStream(BufferWrapper buffer) {
        this.buffer = buffer;
    }
    
    protected int curId;
    protected double curMaxScore;
    
    public void nextEntry() {
    	numberOfTerms++;
    	curId = buffer.getInt();
    	curMaxScore = buffer.getDouble();
    }
    
    public final int getCurId(){
    	return curId;
    }
    
    public final double getCurScore(){
    	return curMaxScore;
    }
    
    public final int numberOfTerms(){
    	return numberOfTerms;
    }
    
    @Override
    public void close() {
    	buffer.close();
    }
}
