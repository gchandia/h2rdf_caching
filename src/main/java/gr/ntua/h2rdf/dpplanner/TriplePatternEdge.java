package gr.ntua.h2rdf.dpplanner;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.indexScans.BGP;


public class TriplePatternEdge implements Comparable<TriplePatternEdge>{
	public String srcPos,edgePos,destPos;
	public String edgeId,destId, signature;
	public List<VarNode> destVars;
	public Integer tripleId;
	private OptimizeOpVisitorDPCaching visitor;
	
	public TriplePatternEdge(OptimizeOpVisitorDPCaching visitor) {
		this.visitor=visitor;
		destVars=new ArrayList<VarNode>();
	}
	
	@Override
	public String toString() {
		return "{"+visitor.tripleIds.get(tripleId)+" sign: "+signature+"}";
	}

	@Override
	public int compareTo(TriplePatternEdge o) {
		return signature.compareTo(o.signature);
	}

	public void computeSignature() {
		signature="$"+srcPos+"0"+"$"+edgePos+edgeId+"$"+destPos+destId;
	}
}
