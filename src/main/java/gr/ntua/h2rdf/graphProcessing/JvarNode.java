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

import java.util.ArrayList;
import java.util.List;

import javaewah.EWAHCompressedBitmap;

public class JvarNode implements Comparable<JvarNode> {

	private String var;
	private List<TriplePattern> triplePatterns;
	private int visited;
	public Integer priority;
	
	public JvarNode(String var) {
		this.var = var;
		triplePatterns = new ArrayList<TriplePattern>();
		visited = 0;
		priority = Integer.MAX_VALUE;
	}
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void visit(){
		visited++;
	}
	
	public boolean isVisited(){
		return visited>0;
	}
	
	public void connect(TriplePattern tp){
		triplePatterns.add(tp);
	}

	@Override
	public String toString() {
		return var ;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((var == null) ? 0 : var.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JvarNode other = (JvarNode) obj;
		if (var == null) {
			if (other.var != null)
				return false;
		} else if (!var.equals(other.var))
			return false;
		return true;
	}

	public List<TriplePattern> getTriplePatterns() {
		return triplePatterns;
	}

	public void setTriplePatterns(List<TriplePattern> triplePatterns) {
		this.triplePatterns = triplePatterns;
	}

	@Override
	public int compareTo(JvarNode o) {
		
		return this.priority.compareTo(o.priority);
	}


}
