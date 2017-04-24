/*******************************************************************************
 * Copyright (c) 2012 Nikos Papailiou. 
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Nikos Papailiou - initial API and implementation
 ******************************************************************************/
package gr.ntua.h2rdf.graphProcessing;

import javaewah.EWAHCompressedBitmap;

import org.jgrapht.graph.DefaultEdge;

public class TPEdge extends DefaultEdge implements Comparable<TPEdge>{

	int visited, processed;
	TriplePattern triplePattern;
	public Integer priority;
	
	public TPEdge(TriplePattern tp) {
		super();
		triplePattern=tp;
		visited =0;
		processed=0;
		priority = Integer.MAX_VALUE;
	}

	public void visit(){
		visited++;
	}

	public boolean isVisited(){
		return visited>0;
	}

	public boolean isProcessed(){
		return processed>0;
	}

	public void process(){
		processed++;
	}

	@Override
	public String toString() {
		return triplePattern + "";
	}

	public void reprocess() {
		processed=0;
		
	}

	@Override
	public int compareTo(TPEdge o) {
		
		return this.priority.compareTo(o.priority);
	}

}
