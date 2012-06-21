package com.ntnu.laika.runstats;

import java.util.Arrays;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class SimpleStats {
	protected static String[] titles;
	protected static long[] counters;
	protected static boolean enabled = false;
	
	public static void init(String... titles){
		SimpleStats.titles = titles;
		int numtitles = titles.length;
		counters = new long[numtitles];
	}
	
	public static void addDescription(int key, int value){
		if (enabled) counters[key] += value;
	}

	public static void enable(){
		enabled = true;
	}
	
	public static void disable(){
		enabled = false;
	}
	
	public static String getString(){
		StringBuffer str = new StringBuffer();
		for(int i=0; i<titles.length; i++){
			str.append(titles[i] + ": " + counters[i] + "\n");
		}
		return str.toString();
	}
	
	public static String getString(int norm){
		StringBuffer str = new StringBuffer();
		for(int i=0; i<titles.length; i++){
			str.append(titles[i] + ": " + (double)counters[i]/norm + "\n");
		}
		return str.toString();
	}
	
	public static String getString(long[] initial_counters, int norm){
		StringBuffer str = new StringBuffer();
		for(int i=0; i<titles.length; i++){
			str.append(titles[i] + ": " + (double)(counters[i]-initial_counters[i])/norm + "\n");
		}
		return str.toString();
	}
	
	public static long[] cloneCounters(){
		return Arrays.copyOf(counters, counters.length);
	}
}
