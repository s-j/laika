package com.ntnu.laika.runstats;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.Arrays;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class TimeResultExtractor {
	
	private static class Entry implements Comparable<Entry>{
		int    key;
		double throughput;
		double latency;
		double procTime;
		
		@Override
		public int compareTo(Entry o) {
			if (key < o.key) return -1;
			else if (key > o.key) return 1;
			else return 0;
		}
	}
	
	public static void main(String args[]) throws Exception{
		//crawl the hierarchy, extract the results
		File folder = new File("/home/simonj/work/enver/resEnver");
	    
	    for (File dir : folder.listFiles(new DirFilter())){
		    System.out.println(dir.getName());
		    File[] files = dir.listFiles(new OutFilter());
		    int cnt = 0;
		    Entry[] entries = new Entry[files.length];
		    for (File file : files){
		    	BufferedReader reader = new BufferedReader(new FileReader(file));
		    	String line, tmp[];
		    	Entry e = new Entry();
		    	//ArrayList<String[]> data = new ArrayList<String[]>();
		    	while((line = reader.readLine()) != null){
		    		if (line.startsWith("run:")){
		    			e.key = Integer.parseInt(line.replace("run:", "").split("/")[2]);
		    		} else if (line.startsWith("res:")){
		    			tmp = line.replace("res:","").split(" ");
		    			if (tmp[0].equals("QPS")){
		    				e.throughput = Double.parseDouble(tmp[1]);
		    			} else if (tmp[0].equals("Lat")){
		    				e.latency = Double.parseDouble(tmp[1]);
		    			} else if (tmp[0].equals("ProcTime")){
		    				e.procTime = Double.parseDouble(tmp[1]);
		    			}
			    	}
		    	}
		    	entries[cnt++] = e;
		    }
		    Arrays.sort(entries, 0, cnt);
		    int spos = 0, len;
		    DecimalFormat df = new DecimalFormat("#.##");
		    while (spos < cnt){
		    	len = 0;
		    	while (len < cnt - spos && entries[spos].key == entries[spos+len].key) len++;
		    	
		    	Entry e = entries[spos];
		    	for (int pos = spos + 1; pos < spos + len; pos++){
		    		e.throughput += entries[pos].throughput;
		    		e.latency += entries[pos].latency;
		    		e.procTime += entries[pos].procTime;
		    	}
		    	e.throughput /= len;
		    	e.latency /= len;
		    	e.procTime /= len;
				
		    	System.out.println(e.key+"\t"+df.format(e.latency)+"\t"+df.format(e.throughput)+
		    			"\t"+df.format(e.procTime));
		    	spos += len;
		    }
		    System.out.println();
	    }
	    
	}

	private static class DirFilter implements FileFilter {
	    public boolean accept(File f) {
	        return f.isDirectory();
	    }
	}
	
	private static class OutFilter implements FileFilter {
	    public boolean accept(File f) {
	        return f.getName().toLowerCase().startsWith("dogsled.o");
	    }
	}
}
