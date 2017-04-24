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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.jgrapht.*;
import org.jgrapht.graph.*;



public final class QueryPreprocess
{
	public static UndirectedGraph<JvarNode, TPEdge> QueryGraph =
            new SimpleGraph<JvarNode, TPEdge>(TPEdge.class);
	public static UndirectedGraph<JvarNode, TPEdge> tree =
            new SimpleGraph<JvarNode, TPEdge>(TPEdge.class);
	
	public static List<DirectedGraph<JvarNode, TPEdge>> cycles = new  LinkedList<DirectedGraph<JvarNode, TPEdge>>();
	public static List<JvarNode> cyclesRoot = new LinkedList<JvarNode>();
	
	public static JvarNode root =null;
	public static int breakFactor = 10000;
	
	
    public static void main(String [] args)
    {
        createGraph();
        
    	Iterator<JvarNode> it = QueryGraph.vertexSet().iterator();
    	while(it.hasNext()){
    		root=it.next();
    		if(root.toString().equals("?y"))
    			break;
    	}

    	System.out.println(QueryGraph.toString());
    	PriorityQueue<JvarNode> openNodes = new PriorityQueue<JvarNode>();
    	root.setPriority(0);
    	
    	while(!openNodes.isEmpty()){
    		//process()
    	}
    	
    	
    	DirectedGraph<JvarNode, TPEdge> path = new DefaultDirectedGraph<JvarNode, TPEdge>(TPEdge.class);
		path.addVertex(root);
		tree.addVertex(root);
		dfsQuery(root, path);
		
    	//createQTree(root, path);
    	
    	
    	
    }

    
 


	private static void dfsQuery(JvarNode father, DirectedGraph<JvarNode, TPEdge> path) {
		if(!father.isVisited()){//process root
			
		}
		else{
			
		}
		Iterator<TPEdge> edgesIt = QueryGraph.edgesOf(father).iterator();
    	
    	while (edgesIt.hasNext()){
    		TPEdge edge = edgesIt.next();
    		//System.out.println(edge.isVisited());
    		if(edge.isVisited()){
    			continue;
    		}
    		else{
    			edge.visit();
    		}
    		
    		JvarNode child = QueryGraph.getEdgeSource(edge);
    		if(child.equals(father)){
        		child=QueryGraph.getEdgeTarget(edge);
    		}

    		if(!child.isVisited()){
    			System.out.println(child+""+child.hashCode());
    			child.visit();
    			
    			tree.addVertex(child);
    			tree.addEdge(father, child, edge);
    			
    			DirectedGraph<JvarNode, TPEdge> newpath = clone(path);
    			System.out.println(father + " "+child);
    			
    			newpath.addVertex(child);
    			newpath.addEdge(father, child, edge);
    			
    			createQTree(child, newpath);
    		}
    		else{
    			//DirectedGraph<JvarNode, TPEdge> cycle = path.removeVertex(arg0)
    			//List<JvarNode> cycle = path.subList(path.indexOf(child), path.size());
    			path.addEdge(father, child, edge);
    			if(!child.equals(root)){
        			JvarNode next = path.getEdgeTarget(path.edgesOf(root).iterator().next());
        			path.removeVertex(root);
        			while(!next.equals(child)){
        				next = path.getEdgeTarget(path.edgesOf(root).iterator().next());
            			path.removeVertex(next);
        			}
    				
    			}
    			cyclesRoot.add(child);
    			cycles.add(path);
    			System.out.println("cycle"+path);
    			//printCycle(cycle);
    			//System.out.println("cycle "+path.substring(path.indexOf(child.toString()))+child.toString());
    			//System.out.println(child.toString()+" tps: "+child.getTriplePatterns().iterator().next());
    		}
    		
    	}
	}





	private static void createQTree(JvarNode father, DirectedGraph<JvarNode, TPEdge> path) {
		
		Iterator<TPEdge> edgesIt = QueryGraph.edgesOf(father).iterator();
    	
    	while (edgesIt.hasNext()){
    		TPEdge edge = edgesIt.next();
    		//System.out.println(edge.isVisited());
    		if(edge.isVisited()){
    			continue;
    		}
    		else{
    			edge.visit();
    		}
    		
    		JvarNode child = QueryGraph.getEdgeSource(edge);
    		if(child.equals(father)){
        		child=QueryGraph.getEdgeTarget(edge);
    		}

    		if(!child.isVisited()){
    			System.out.println(child+""+child.hashCode());
    			child.visit();
    			
    			tree.addVertex(child);
    			tree.addEdge(father, child, edge);
    			
    			DirectedGraph<JvarNode, TPEdge> newpath = clone(path);
    			System.out.println(father + " "+child);
    			
    			newpath.addVertex(child);
    			newpath.addEdge(father, child, edge);
    			
    			createQTree(child, newpath);
    		}
    		else{
    			//DirectedGraph<JvarNode, TPEdge> cycle = path.removeVertex(arg0)
    			//List<JvarNode> cycle = path.subList(path.indexOf(child), path.size());
    			path.addEdge(father, child, edge);
    			if(!child.equals(root)){
        			JvarNode next = path.getEdgeTarget(path.edgesOf(root).iterator().next());
        			path.removeVertex(root);
        			while(!next.equals(child)){
        				next = path.getEdgeTarget(path.edgesOf(root).iterator().next());
            			path.removeVertex(next);
        			}
    				
    			}
    			cyclesRoot.add(child);
    			cycles.add(path);
    			System.out.println("cycle"+path);
    			//printCycle(cycle);
    			//System.out.println("cycle "+path.substring(path.indexOf(child.toString()))+child.toString());
    			//System.out.println(child.toString()+" tps: "+child.getTriplePatterns().iterator().next());
    		}
    		
    	}
	}



	

	private static DirectedGraph<JvarNode, TPEdge> clone(
			DirectedGraph<JvarNode, TPEdge> path) {
		DirectedGraph<JvarNode, TPEdge> ret = new DefaultDirectedGraph<JvarNode, TPEdge>(TPEdge.class);
		Iterator<JvarNode> it = path.vertexSet().iterator();
		while(it.hasNext()){
			ret.addVertex(it.next());
		}
		Iterator<TPEdge> it2 = path.edgeSet().iterator();
		while(it2.hasNext()){
			TPEdge temp = it2.next();
			ret.addEdge(path.getEdgeSource(temp),path.getEdgeTarget(temp), temp);
		}
		return ret;
	}



	private static void createGraph() {

            TriplePattern tp1 = new TriplePattern("?x rdf:type ub:GraduateStudent");
            TriplePattern tp2 = new TriplePattern("?y rdf:type ub:University");
            TriplePattern tp3 = new TriplePattern("?z rdf:type ub:Department");
            TriplePattern tp4 = new TriplePattern("?x ub:memberOf ?z");
            TriplePattern tp5 = new TriplePattern("?z ub:subOrganizationOf ?y");
            TriplePattern tp6 = new TriplePattern("?x ub:undergraduateDegreeFrom ?y");
            
            
            JvarNode x = new JvarNode("?x");
            x.connect(tp1);
            JvarNode y = new JvarNode("?y");
            y.connect(tp2);
            JvarNode z = new JvarNode("?z");
            z.connect(tp3);
            

            /*JvarNode w1 = new JvarNode("w1");
            JvarNode w2 = new JvarNode("w2");
            JvarNode w3 = new JvarNode("w3");
            JvarNode w4 = new JvarNode("w4");*/
            
            QueryGraph.addVertex(x);
            QueryGraph.addVertex(y);
            QueryGraph.addVertex(z);
            /*JvarGraph.addVertex(w1);
            JvarGraph.addVertex(w2);
            JvarGraph.addVertex(w3);
            JvarGraph.addVertex(w4);*/
            
            QueryGraph.addEdge(x, z, new TPEdge(tp4));
            QueryGraph.addEdge(z, y, new TPEdge(tp5));
            QueryGraph.addEdge(x, y, new TPEdge(tp6));
            
            /*JvarGraph.addEdge(z, w2, new TPEdge());
            JvarGraph.addEdge(z, w3, new TPEdge());
            JvarGraph.addEdge(y, w1, new TPEdge());
            JvarGraph.addEdge(y, w4, new TPEdge());
            JvarGraph.addEdge(w3, w1, new TPEdge());
            JvarGraph.addEdge(w1, w4, new TPEdge());*/
            
	}

}
