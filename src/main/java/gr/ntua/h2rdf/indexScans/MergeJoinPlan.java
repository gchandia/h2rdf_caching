package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.dpplanner.CachedResult;
import gr.ntua.h2rdf.dpplanner.CachedResults;
import gr.ntua.h2rdf.dpplanner.DPJoinPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MergeJoinPlan {
	public double cost;
	public boolean centalized;
	public Set<BGP> scans;
	public List<CachedResult> resultScans;
	public Set<ResultBGP> intermediate;
	public long[][] maxPartition;
	public DPJoinPlan maxPattern;
	
	public String toString(){
		String ret = "";
		if(centalized){
			ret+= "Centralized ";
		}
		else{
			ret+= "MapReduce ";
		}
		ret+=  "Cost: "+cost+"\n";
		ret+=  "Partitions: "+maxPartition.length+"\n";
		ret+=  "Partition BGP: "+maxPattern.print()+"\n";
		ret+="Scans\n";
		for(BGP b : scans){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		ret+="Intermediate\n";
		for(ResultBGP b : intermediate){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		return ret;
	}

	public MergeJoinPlan() {
		scans = new HashSet<BGP>();
		resultScans = new ArrayList<CachedResult>();
		intermediate = new HashSet<ResultBGP>();
		cost=0;
	}
}
