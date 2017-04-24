package gr.ntua.h2rdf.indexScans;

import java.util.Set;

public class JoinPlan {
	public double cost;
	public boolean centalized;
	public Set<ResultBGP> Map, Reduce;
	
	public String toString(){
		String ret = "";
		if(centalized){
			ret+= "Centralized ";
		}
		else{
			ret+= "MapReduce ";
		}
		ret+=  "Cost: "+cost+"\n";
		ret+="Map\n";
		for(ResultBGP b : Map){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		ret+="Reduce\n";
		for(ResultBGP b : Reduce){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		return ret;
	}
}
