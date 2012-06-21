package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;

import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.Statistics;
import com.ntnu.laika.structures.fastmaxscore.FastMaxScore;
import com.ntnu.laika.structures.lexicon.Lexicon;
import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.LexiconInputStream;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class MSDPartitioning {
	public static int[] finalmap;
	
	public static class TermEntry implements Comparable<TermEntry>{
		public int termID;
		public double maxScore;
		public long listLength;
		
		public TermEntry(int termID, double maxScore, long listLength){
			this.termID = termID;
			this.maxScore = maxScore;
			this.listLength = listLength;
		}
		
		@Override
		public int compareTo(TermEntry o) {
			if (maxScore > o.maxScore) return -1;
			else if (maxScore < o.maxScore) return 1;
			else return 0;
		}
	}
	
	public static class TermAssignment implements Comparable<TermAssignment>{
		private int termID;
		private int nodeID;
		
		public TermAssignment(int termID, int nodeID){
			this.termID = termID;
			this.nodeID = nodeID;
		}
		
		@Override
		public int compareTo(TermAssignment o) {
			if (termID> o.termID) return 1;
			else if (termID < o.termID) return -1;
			else return 0;
		}
	}
	
	public static void partitionBySize(Index index, int numParts){
		Statistics	stats = index.getStatistics();
		int numTerms = stats.getNumberOfUniqueTerms();
		
		Lexicon lexicon = index.getLexicon(numTerms);
		long partitionLimit = lexicon.getEndOffset()/numParts;
		System.out.println("Partition size limit: " + partitionLimit);
		lexicon.close();
		
		TermEntry[] entries = new TermEntry[numTerms];
		TermAssignment[] assignments = new TermAssignment[numTerms];
		index.loadFastMaxScores(numTerms);
		
		LexiconEntry lEntry;
		
		LexiconInputStream lexIn = index.getLexiconInputStream(stats.getNumberOfUniqueTerms());
		for (int i=0; i<numTerms; i++){
			lEntry = lexIn.nextEntry();
			entries[i] = new TermEntry(lEntry.getTermId(), FastMaxScore.getMaxScore(lEntry.getTermId()), lEntry.getEndOffset() - lEntry.getStartOffset());
			if (i % 5000000 == 0) System.out.println(i);
		}		
		lexIn.close();
		
		System.out.println("done reading!");
		Arrays.sort(entries);
		System.out.println("done sorting!");
		long partitionSize = 0; 
		int partitionCount = 0;
		int partitionNo = 0;
		
		TermEntry tEntry;
		System.out.println(entries[0].maxScore);
		for (int i=0; i<numTerms; i++){
			
			if (partitionSize >= partitionLimit && partitionNo < numParts - 1){
				System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
				System.out.println(entries[i].maxScore);
				partitionNo++; partitionSize = 0; partitionCount = 0;
			}
			//termMap.put(e.termID, partitionNo);
			tEntry = entries[i];
			assignments[i] = new TermAssignment(tEntry.termID, partitionNo);
			partitionSize += tEntry.listLength;
			partitionCount++;
			if (i % 5000000 == 0) System.out.println(i);
		}
		
		System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
		System.out.println(entries[numTerms-1].maxScore);
		
		System.out.println("done splitting!");
		Arrays.sort(assignments);
		System.out.println("done resorting!");
		
		finalmap = new int[numTerms];
		for (int i=0; i<numTerms; i++) finalmap[i] = assignments[i].nodeID;		
	}
	
	public static void partitionByLoad(Index index, int numParts){
		HashMap<String, Integer> cntmap  = new HashMap<String, Integer>();
		try {
			BufferedReader reader;
			reader = new BufferedReader(new FileReader("/home/simonj/countsfull"));
			String line,tmp[];
			while ((line = reader.readLine()) != null ) {
				tmp = line.split("\t");
				cntmap.put(tmp[0], Integer.parseInt(tmp[1]));
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Statistics	stats = index.getStatistics();
		int numTerms = stats.getNumberOfUniqueTerms();
		long partitionLimit = 0, l;
		TermEntry[] entries = new TermEntry[numTerms];
		TermAssignment[] assignments = new TermAssignment[numTerms];
		index.loadFastMaxScores(numTerms);
		
		LexiconEntry lEntry;
		LexiconInputStream lexIn = index.getLexiconInputStream(stats.getNumberOfUniqueTerms());
		for (int i=0; i<numTerms; i++){
			lEntry = lexIn.nextEntry();
			Integer cnt = cntmap.get(lEntry.getTerm());
			l = ((cnt!=null) ? cnt + 1 : 1) * (lEntry.getEndOffset() - lEntry.getStartOffset());
			//l = (lEntry.getEndOffset() - lEntry.getStartOffset());
			entries[i] = new TermEntry(lEntry.getTermId(), FastMaxScore.getMaxScore(lEntry.getTermId()), l);
			partitionLimit += l;
			if (partitionLimit<0) System.out.println("bugbug!");
			if (i % 5000000 == 0) System.out.println(i);
		}		
		lexIn.close();
		//System.out.println("--->"+partitionLimit+":"+check);
		partitionLimit /= numParts;
		System.out.println("Partition load limit: " + partitionLimit);
		System.out.println("done reading!");
		//System.exit(0);
		Arrays.sort(entries);
		System.out.println("done sorting!");
		long partitionSize = 0; 
		int partitionCount = 0;
		int partitionNo = 0;
		
		TermEntry tEntry;
		System.out.println(entries[0].maxScore);
		for (int i=0; i<numTerms; i++){
			tEntry = entries[i];
			if (partitionSize + tEntry.listLength > partitionLimit && partitionNo < numParts - 1){
				System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
				System.out.println(entries[i].maxScore);
				partitionNo++; partitionSize = 0; partitionCount = 0;
			}
			//termMap.put(e.termID, partitionNo);
			assignments[i] = new TermAssignment(tEntry.termID, partitionNo);
			partitionSize += tEntry.listLength;
			partitionCount++;
			if (i % 5000000 == 0) System.out.println(i);
		}
		
		System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
		System.out.println(entries[numTerms-1].maxScore);
		
		System.out.println("done splitting!");
		Arrays.sort(assignments);
		System.out.println("done resorting!");
		
		finalmap = new int[numTerms];
		for (int i=0; i<numTerms; i++){
			finalmap[i] = assignments[i].nodeID;		
		}
	}
	
	
	public static void partitionByCount(Index index, int numParts){
		Statistics	stats = index.getStatistics();
		int numTerms = stats.getNumberOfUniqueTerms();
		
		Lexicon lexicon = index.getLexicon(numTerms);
		long partitionLimit = numTerms/numParts;
		System.out.println("Partition count limit: " + partitionLimit);
		lexicon.close();
		
		TermEntry[] entries = new TermEntry[numTerms];
		TermAssignment[] assignments = new TermAssignment[numTerms];
		index.loadFastMaxScores(numTerms);
		
		LexiconEntry lEntry;
		//msmap = new double[numTerms];
		LexiconInputStream lexIn = index.getLexiconInputStream(stats.getNumberOfUniqueTerms());
		for (int i=0; i<numTerms; i++){
			lEntry = lexIn.nextEntry();
			entries[i] = new TermEntry(lEntry.getTermId(), FastMaxScore.getMaxScore(lEntry.getTermId()), lEntry.getEndOffset() - lEntry.getStartOffset());

			//msmap[i]=FastMaxScore.getMaxScore(lEntry.getTermId());
			if (i % 5000000 == 0) System.out.println(i);
		}

		lexIn.close();
		System.out.println("done reading!");
		Arrays.sort(entries);
		System.out.println("done sorting!");
		long partitionSize = 0; 
		int partitionCount = 0;
		int partitionNo = 0;
		
		TermEntry tEntry;
		System.out.println(entries[0].maxScore);
		for (int i=0; i<numTerms; i++){
			
			if (partitionCount >= partitionLimit && partitionNo < numParts - 1){
				System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
				System.out.println(entries[i].maxScore);
				partitionNo++; partitionSize = 0; partitionCount = 0;
			}
			//termMap.put(e.termID, partitionNo);
			tEntry = entries[i];
			assignments[i] = new TermAssignment(tEntry.termID, partitionNo);
			partitionSize += tEntry.listLength;
			partitionCount++;
			if (i % 5000000 == 0) System.out.println(i);
		}
		
		System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
		System.out.println(entries[numTerms-1].maxScore);
		
		System.out.println("done splitting!");
		Arrays.sort(assignments);
		System.out.println("done resorting!");
		//for (int i=0; i<50; i++) System.out.println(assignments[i].termID + " " + assignments[i].nodeID);
		//return termMap;
		finalmap = new int[numTerms];
		for (int i=0; i<numTerms; i++){
			finalmap[i] = assignments[i].nodeID;	
		}
	}
	
	public static void partitionByCountAndSize(Index index, int numParts){
		Statistics	stats = index.getStatistics();
		int numTerms = stats.getNumberOfUniqueTerms();
		
		Lexicon lexicon = index.getLexicon(numTerms);
		long partitionCountLimit = (long) (3.0 * numTerms/numParts);
		long partitionSizeLimit =  (long) (1.1 * lexicon.getEndOffset()/numParts);
		lexicon.close();		
		System.out.println("Partition size limit: " + partitionSizeLimit);
		System.out.println("Partition count limit: " + partitionCountLimit);

		
		TermEntry[] entries = new TermEntry[numTerms];
		TermAssignment[] assignments = new TermAssignment[numTerms];
		index.loadFastMaxScores(numTerms);
		
		LexiconEntry lEntry;
		
		LexiconInputStream lexIn = index.getLexiconInputStream(stats.getNumberOfUniqueTerms());
		for (int i=0; i<numTerms; i++){
			lEntry = lexIn.nextEntry();
			entries[i] = new TermEntry(lEntry.getTermId(), FastMaxScore.getMaxScore(lEntry.getTermId()), lEntry.getEndOffset() - lEntry.getStartOffset());
			if (i % 5000000 == 0) System.out.println(i);
		}		
		lexIn.close();
		System.out.println("done reading!");
		Arrays.sort(entries);
		System.out.println("done sorting!");
		long partitionSize = 0; 
		int partitionCount = 0;
		int partitionNo = 0;
		
		TermEntry tEntry;
		System.out.println(entries[0].maxScore);
		for (int i=0; i<numTerms; i++){
			if ((partitionCount >= partitionCountLimit || partitionSize >= partitionSizeLimit) && partitionNo < numParts - 1){
				System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
				System.out.println(entries[i].maxScore);
				partitionNo++; partitionSize = 0; partitionCount = 0;
			}
			//termMap.put(e.termID, partitionNo);
			tEntry = entries[i];
			assignments[i] = new TermAssignment(tEntry.termID, partitionNo);
			partitionSize += tEntry.listLength;
			partitionCount++;
			if (i % 5000000 == 0) System.out.println(i);
		}
		
		System.out.println(partitionNo + "\t" + partitionCount + "\t" + partitionSize);
		System.out.println(entries[numTerms-1].maxScore);
		
		System.out.println("done splitting!");
		Arrays.sort(assignments);
		System.out.println("done resorting!");
		//for (int i=0; i<50; i++) System.out.println(assignments[i].termID + " " + assignments[i].nodeID);
		//return termMap;
		finalmap = new int[numTerms];
		for (int i=0; i<numTerms; i++){
			finalmap[i] = assignments[i].nodeID;	
		}
	}
	
	public static int getNodeID(int termID){
		if (termID < 0 || termID>finalmap.length) return -1;
		return finalmap[termID];
	}
	
	public static void printMaxScoresVsLengths(Index index) throws Exception{
		Statistics	stats = index.getStatistics();
		int numTerms = stats.getNumberOfUniqueTerms();
			
		index.loadFastMaxScores(numTerms);
		
		LexiconEntry lEntry;
		
		LexiconInputStream lexIn = index.getLexiconInputStream(stats.getNumberOfUniqueTerms());
		BufferedWriter buffo = new BufferedWriter(new FileWriter("/home/simonj/dots.txt"));
		for (int i=0; i<numTerms; i++){
			lEntry = lexIn.nextEntry();
			buffo.write(FastMaxScore.getMaxScore(lEntry.getTermId()) + " " + lEntry.getN_t()+"\n");
		}		
		buffo.flush(); buffo.close();
		lexIn.close();
		
	}
	
	public static void main(String[] args) throws Exception{
		Index sIndex = new Index("/media/61dd738a-1473-42a4-8ae4-09760ec2b03f/data/laika_v1/laikatest");
		partitionByCount(sIndex, 8);
		sIndex.close();
		//printMaxScoresVsLengths(sIndex);
	}
}

