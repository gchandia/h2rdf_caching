package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;


public interface DPJoinPlan extends Comparable<DPJoinPlan>{
	
	public String print();
	public void execute(OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor) throws Exception;
	public Double getCost();
	public double[] getStatistics (Integer joinVar) throws IOException;
	public List<ResultBGP> getResults () throws IOException;
	public void computeCost() throws IOException;
	public List<Integer> getOrdering();
}
