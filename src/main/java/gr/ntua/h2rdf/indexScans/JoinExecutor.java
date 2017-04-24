package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.client.H2RDFConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class JoinExecutor {
	public static int joinId=0;
	
	public static List<ResultBGP> execute(Map<Var, JoinPlan> m, HTable table, HTable indexTable, boolean centalized) throws Exception {
		
		System.out.println(m);
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedJoinExecutor.execute(m,  table,  indexTable);
		}
		else{
			ret = MapReduceJoinExecutor.execute( m,  table,  indexTable);
		}
		joinId++;
		
		return ret;
	}

	public static List<ResultBGP> executeMerge(MergeJoinPlan plan,
			Var joinVar, HTable table, HTable indexTable, boolean centalized) throws Exception {
		System.out.println(plan);
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedMergeJoinExecutor.execute(plan,joinVar,  table,  indexTable,null);
		}
		else{
			ret = MapReduceMergeJoinExecutor1.execute( plan,joinVar,  table,  indexTable, null);
		}
		joinId++;
		return ret;
	}

	public static List<ResultBGP> executeMerge1(MergeJoinPlan plan,
			Var joinVar, HTable table, HTable indexTable, boolean centalized,
			OptimizeOpVisitorDPCaching visitor) throws Exception{
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedMergeJoinExecutor.execute(plan,joinVar,  table,  indexTable,visitor);
		}
		else{
			ret = MapReduceMergeJoinExecutor1.execute( plan,joinVar,  table,  indexTable, visitor);
		}
		visitor.cachingExecutor.tid++;
		
		return ret;
	}

	public static List<ResultBGP> executeOrdering(ResultBGP resultBGP,
			long[][] maxPartition, List<Integer> orderVarsInt, boolean centralized, String table, OptimizeOpVisitorDPCaching visitor) throws Exception {
		List<ResultBGP> ret= null;
		if(centralized){
			ret = CentralizedOrderingExecutor.execute(resultBGP, maxPartition, orderVarsInt, table, visitor);
		}
		else{
			ret = MROrderingExecutor.execute(resultBGP, maxPartition, orderVarsInt, table, visitor);
		}
		visitor.cachingExecutor.tid++;
		return ret;
	}
	
	

}
