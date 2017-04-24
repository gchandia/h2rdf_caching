package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class GroupBy implements DPJoinPlan{
	public DPJoinPlan subplan;
	private List<Var> groupVars;
	private OptimizeOpVisitorDPCaching visitor;
	private CachingExecutor cachingExecutor;

	public GroupBy(List<Var> groupVars, OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) {
		this.visitor =visitor;
		this.groupVars = groupVars;
		this.cachingExecutor = cachingExecutor;
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String print() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Double getCost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[] getStatistics(Integer joinVar) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ResultBGP> getResults() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void computeCost() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Integer> getOrdering() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setEdgeGraph(BitSet s) {
		// TODO Auto-generated method stub
		
	}
	
	
}
