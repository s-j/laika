package com.ntnu.laika.structures;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Statistics {
	/*
	docno.byte.length=20
	index.inverted.class=uk.ac.gla.terrier.structures.InvertedIndex
	index.inverted.parameter_values=lexicon,path,prefix
	*/
	
	private int numberOfDocuments;
	private int numberOfUniqueTerms;
	private long numberOfPointers;
	private long numberOfTokens;
	private double averageDocumentLength;
	
	public Statistics(int numberOfDocuments, int numberOfUniqueTerms, long numberOfPointers, long numberOfTokens){
		this.numberOfDocuments = numberOfDocuments;
		this.numberOfUniqueTerms = numberOfUniqueTerms;
		this.numberOfPointers = numberOfPointers;
		this.numberOfTokens = numberOfTokens;
		
		if (numberOfDocuments != 0)
			averageDocumentLength =
				(1.0D * numberOfTokens) / (1.0D * numberOfDocuments);
		else
			averageDocumentLength = 0.0D;
	}

	public void setNumberOfDocuments(int numberOfDocuments) {
		this.numberOfDocuments = numberOfDocuments;
	}

	public void setNumberOfUniqueTerms(int numberOfUniqueTerms) {
		this.numberOfUniqueTerms = numberOfUniqueTerms;
	}

	public void setNumberOfPointers(long numberOfPointers) {
		this.numberOfPointers = numberOfPointers;
	}

	public void setNumberOfTokens(long numberOfTokens) {
		this.numberOfTokens = numberOfTokens;
	}

	public void setAverageDocumentLength() {
		if (numberOfDocuments != 0)
			averageDocumentLength =
				(1.0D * numberOfTokens) / (1.0D * numberOfDocuments);
		else
			averageDocumentLength = 0.0D;
	}

	public int getNumberOfDocuments() {
		return numberOfDocuments;
	}

	public int getNumberOfUniqueTerms() {
		return numberOfUniqueTerms;
	}

	public long getNumberOfPointers() {
		return numberOfPointers;
	}

	public long getNumberOfTokens() {
		return numberOfTokens;
	}

	public double getAverageDocumentLength() {
		return averageDocumentLength;
	}
}
