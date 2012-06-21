/*
 * Terrier - Terabyte Retriever 
 * Webpage: http://ir.dcs.gla.ac.uk/terrier 
 * Contact: terrier{a.}dcs.gla.ac.uk
 * University of Glasgow - Department of Computing Science
 * http://www.gla.ac.uk/
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is BM25.java.
 *
 * The Original Code is Copyright (C) 2004-2009 the University of Glasgow.
 * All Rights Reserved.
 *
 * Contributor(s):
 *   Gianni Amati <gba{a.}fub.it> (original author)
 *   Ben He <ben{a.}dcs.gla.ac.uk> 
 *   Vassilis Plachouras <vassilis{a.}dcs.gla.ac.uk>
 */
package com.ntnu.laika.query.processing.scoremodels;

/**
 * Okapi BM25 weighting model based on the implementation in Terrier
 * @author Gianni Amati, Ben He, Vassilis Plachouras, Simon Jonassen, Barla Cambazoglu
 */
public class BM25Barla extends WeightingModel {

	private static final double k_1 = 1.2d;
	private static final double k_3 = 8d;
	private static final double b = 0.75d;
	
	public BM25Barla() {
		super();
	}

	/**
	 * Returns the name of the model.
	 * @return the name of the model
	 */
	public final String getInfo() {
		return "BM25b"+b;
	}
	/**
	 * Uses BM25 to compute a weight for a term in a document.
	 * @param tf The term frequency in the document
	 * @param docLength the document's length
	 * @return the score assigned to a document with the given 
	 *         tf and docLength, and other preset parameters
	 */
	public final double score(double tf, double docLength) {
	    return  (tf * (k_3 + 1d) * keyFrequency / ((k_3 + keyFrequency)*(k_1 * ((1 - b) + b * docLength / averageDocumentLength) + tf))) 
	            * (log((numberOfDocuments - documentFrequency + 0.5d) / (documentFrequency + 0.5d)));
	}
	
	
	public final double precompute(){
		return log((numberOfDocuments - documentFrequency + 0.5d) / (documentFrequency + 0.5d));
	}
	
	public final double score(double tf, double docLength, double c1) {
	    double K = k_1 * ((1 - b) + b * docLength / averageDocumentLength) + tf;
	    return (tf * (k_3 + 1d) * keyFrequency / ((k_3 + keyFrequency) * K)) * c1;
	}

	/**
	 * Uses BM25 to compute a weight for a term in a document.
	 * @param tf The term frequency in the document
	 * @param docLength the document's length
	 * @param documentFrequency The document frequency of the term
	 * @param termFrequency the term frequency in the collection
	 * @param keyFrequency the term frequency in the query
	 * @return the score assigned by the weighting model BM25.
	 */
	public final double score(
		double tf,
		double docLength,
		double documentFrequency,
		double termFrequency,
		double keyFrequency) {

	    return (tf * (k_3 + 1d) * keyFrequency / ((k_3 + keyFrequency)*(k_1 * ((1 - b) + b * docLength / averageDocumentLength) + tf)))
	            * (log((numberOfDocuments - documentFrequency + 0.5d) / (documentFrequency + 0.5d)));
	}
}