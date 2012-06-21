package com.ntnu.laika.structures;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LocalIndex extends Index {
	private Statistics localStats = null;
	
	public LocalIndex(String path){
		super(path);
		
		localStats = new Statistics(
			Integer.parseInt(indexProperties.getProperty("index.localNumberOfDocuments", "0")),
			Integer.parseInt(indexProperties.getProperty("index.localNumberOfUniqueTerms", "0")),
			Long.parseLong(indexProperties.getProperty("index.localNumberOfPointers", "0")),
			Long.parseLong(indexProperties.getProperty("index.localNumberOfTokens", "0")));
	}
	
	public Statistics getLocalStatistics(){
		return localStats;
	}
	
	public void setLocalStatistics(Statistics newGlobalStats){
		localStats = newGlobalStats;
	}
	
	/**
	 *@Override
	 */
	protected void storeStatsAndSettings(){
			indexProperties.setProperty("index.localNumberOfDocuments", ""+localStats.getNumberOfDocuments());
			indexProperties.setProperty("index.localNumberOfUniqueTerms", ""+localStats.getNumberOfUniqueTerms());
			indexProperties.setProperty("index.localNumberOfPointers", ""+localStats.getNumberOfPointers());
			indexProperties.setProperty("index.localNumberOfTokens", ""+localStats.getNumberOfTokens());
			super.storeStatsAndSettings();
	}
}
