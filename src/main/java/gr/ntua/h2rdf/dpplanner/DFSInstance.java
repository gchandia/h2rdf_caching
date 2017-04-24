package gr.ntua.h2rdf.dpplanner;

import java.util.BitSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

public class DFSInstance implements Comparable<DFSInstance>{
	public String signature;
	public boolean[] visitedTriples, visitedVars;
	public TreeMap<VarNode, PriorityQueue<TriplePatternEdge>> graph;
	private List<DFSInstance> list;
	private VarNode root;
	
	
	public DFSInstance(TreeMap<VarNode, PriorityQueue<TriplePatternEdge>> graph,
			OptimizeOpVisitorDPCaching visitor, VarNode root, List<DFSInstance> list) {
		this.graph=graph;
		this.list=list;
		this.root=root;
		signature="";
		visitedTriples = new boolean[visitor.numTriples];
		visitedVars = new boolean[visitor.numVars];
	}
	
	public String runDFS(){
		processRoot(root);
		return signature;
	}

	private void processRoot(VarNode root) {
		PriorityQueue<TriplePatternEdge> edges = graph.get(root);
		for(TriplePatternEdge e : edges){
			
		}
	}

	@Override
	public int compareTo(DFSInstance o) {
		return signature.compareTo(o.signature);
	}

}
