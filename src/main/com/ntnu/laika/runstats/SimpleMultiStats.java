package com.ntnu.laika.runstats;

import java.text.DecimalFormat;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class SimpleMultiStats {
	protected static String[] rowtitles;
	protected static String[] coltitles;
	protected static long[][] counters;
	protected static boolean enabled = false;
	protected static int rownum = 0;
	
	public static void init(String[] rowtitles, String... coltitles){
		SimpleMultiStats.rowtitles = rowtitles;
		SimpleMultiStats.coltitles = coltitles;
		counters = new long[rowtitles.length][coltitles.length];
		enabled = true;
	}
	
	public static void setContext(int num){
		rownum = num;
	}
	
	public static void addDescription(int key, int value){
		if (enabled) counters[rownum][key] += value;
	}

	public static synchronized void synchronizedAddDescription(int key, int value){
		if (enabled) counters[rownum][key] += value;
	}
	
	public static void disable(){
		enabled = false;
	}
	
	public static void enable(){
		enabled = true;
	}
	
	private static DecimalFormat df = new DecimalFormat("#.#");
	
	public static String getString(int norms[]){
		StringBuffer str = new StringBuffer("###");
		for(int i=0; i<coltitles.length; i++) {
			str.append('\t');
			str.append(coltitles[i]);
		}
		str.append('\n');
		for (int i=0; i<counters.length; i++){
			str.append(rowtitles[i]);
			for (int j=0; j<coltitles.length; j++){
				str.append('\t');
				str.append(norms[i]>0?df.format((double)counters[i][j]/norms[i]):"-");
			}
			str.append('\n');
		}
		return str.toString();
	}
}
