package com.ntnu.laika.runstats;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Runstats {
	protected static HashMap<String, HashMap<Integer, Integer>> stats = new HashMap<String, HashMap<Integer, Integer>>();
			
	public synchronized static void addDescription(String descr, int key, int value, boolean increment){
		HashMap<Integer, Integer> group = stats.get(descr);
		
		if (group == null){
			group = new HashMap<Integer, Integer>();
			group.put(key, value);
			stats.put(descr, group);
			return;
		}
		
		if (increment){
			Integer val = group.get(key);
			group.put(key, val != null ? val + value : value);
		} else {
			group.put(key, value);
		}
	}
	
	public static String getString(){
		StringBuffer str = new StringBuffer();
		for (Entry<String, HashMap<Integer, Integer>> group : stats.entrySet()){
			str.append("=====\t" + group.getKey() + "\t=====\n");
			for(Entry<Integer,Integer> keyval : group.getValue().entrySet()){
				str.append(keyval.getKey() + ": " + keyval.getValue() + "\n");
			}
		}		
		return str.toString();
	}
}
