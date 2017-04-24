package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;
import com.hp.hpl.jena.sparql.core.Var;


public class ResultBGP {
	public Triple bgp;
	public Set<Var> joinVars;
	public boolean isJoined;
	public long size;
	public Path path;
	public Map<Integer,double[]> stats;
	public HashMap<Integer, Integer> varRelabeling;//key:file varId, value newqueryVarId
	public HashMap<Integer, Long> selectiveBindings;
	public HashMap<Integer, long[][]> partitions;
	
	public String print(){
		String ret = "";
		for(Var v : joinVars)
			ret+=v+" ";
		return ret;
	}
	public ResultBGP(){
		
	}
	
	
	public ResultBGP(Set<Var> vars, Path path, Map<Integer,double[]> stats)  {
		isJoined = false;
		joinVars = vars;
		this.path = path;
		this.stats = stats;
	}
	
	/*
	 * ret[0] : ni join bindings for joinVar
	 * ret[1] : oi average bindings for each joinVar binding
	 */
	public double[] getStatistics (Var joinVar) throws IOException {
		return stats.get(OptimizeOpVisitorMergeJoin.varIds.get(joinVar).intValue());
	}
	public double[] getStatistics(Var joinVar,
			OptimizeOpVisitorDPCaching visitor) {
		return stats.get(visitor.varRevIds.get(joinVar).intValue());
	}
	public HashMap<Integer, long[][]> getPartitions() {
		return partitions;
	}
	public void setPartitions(HashMap<Integer, long[][]> partitions) {
		this.partitions = partitions;
	}
	
}
