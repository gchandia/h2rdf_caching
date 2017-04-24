package gr.ntua.h2rdf.dpplanner;

//package jbliss;

import java.util.*;

import fi.tkk.ics.jbliss.Digraph;
import fi.tkk.ics.jbliss.DefaultReporter;
import fi.tkk.ics.jbliss.Utils;

/**
 * A simple example of how to use jbliss.
 * Generates a small graph and finds its automorphisms and canonical form.
 */
public class TestJBliss
{
    public static void main(String args[])
    {
    	//System.out.println(System.getProperty("java.library.path"));
    	DefaultReporter reporter = new DefaultReporter();
		/* Create the graph */
    	long start1 = System.nanoTime();
		Digraph<Integer> g = new Digraph<Integer>();
		g.add_vertex(0);
		g.add_vertex(1);
		g.add_vertex(2);
		g.add_vertex(3,1);
		g.add_vertex(4,1);
		g.add_vertex(5,1);
		
		g.add_vertex(6,2);
		g.add_vertex(7,2);
		g.add_vertex(8,2);
		g.add_vertex(9,3);
		g.add_vertex(10,3);
		g.add_vertex(11,3);
		
		//vertical edges
		g.add_edge(0,6);
		g.add_edge(1,7);
		g.add_edge(2,8);
		g.add_edge(3,9);
		g.add_edge(4,10);
		g.add_edge(5,11);
		
		//horizontal edges firts level SS
		g.add_edge(0,5);
		g.add_edge(1,3);
		g.add_edge(2,4);
	    g.add_edge(3,1);
	    g.add_edge(4,2);
	    g.add_edge(5,0);
		
		//horizontal edges second level SO
		g.add_edge(6,8);
		g.add_edge(7,6);
		g.add_edge(8,7);
	    g.add_edge(9,6);
	    g.add_edge(10,7);
	    g.add_edge(11,8);
    	long stop1 = System.nanoTime();

	    Map<Integer,Integer> canlab=null;
	    /*Map<Integer,Integer> rel = new HashMap<Integer, Integer>();
	    rel.put(0,2);
	    rel.put(1,0);
	    rel.put(2,3);
	    rel.put(3,1);
	    g = g.relabel(rel);
	    for (int i = 0; i < 100; i++) {
			canlab = g.canonical_labeling();
			System.out.println(i);
		}*/
	    
		/* Print the graph */
		System.out.println("The graph is:");
		g.write_dot(System.out);
		/* Find (a generating set for) the automorphism group of the graph */
		//g.find_automorphisms(reporter, null);
		/* Compute the canonical labeling */
    	long start = System.nanoTime();
	    canlab = g.canonical_labeling();
    	long stop = System.nanoTime();
		/* Print the canonical labeling */
		System.out.print("A canonical labeling for the graph is: ");
		Utils.print_labeling(System.out, canlab);
		
		System.out.println("");
		/* Compute the canonical form */
		Digraph<Integer> g_canform = g.relabel(canlab);
		/* Print the canonical form of the graph */
		System.out.println("The canonical form of the graph is:");
		g_canform.write_dot(System.out);
    	System.out.println("Graph creation time us: "+(stop1-start1)/1000);
    	System.out.println("Canonization total time us: "+(stop-start)/1000);
    }
}
