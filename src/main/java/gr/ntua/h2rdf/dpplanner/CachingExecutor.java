package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.indexScans.PartitionFinder;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCachingOld;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCachingStars;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCachingStars2;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorLabelOld;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorLabelStars;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorLabelStars2;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;

public class CachingExecutor {

	public static HashMap<String, HTable> tables = new HashMap<String, HTable>();
	public static HashMap<String, HTable> indexTables = new HashMap<String, HTable>();
	public static HashMap<String, PartitionFinder> partitionFinders = new HashMap<String, PartitionFinder>();
	
	private static Configuration hconf ;
	private String table;
	public int id, tid;
	private HTable t, indexT;
	private PartitionFinder partitionFinder;
	public FileSystem fs;
	public OptimizeOpVisitorDPCaching visitor;
	private boolean old;
	
	public CachingExecutor(String table, int id, boolean old) {
		this.old=old;
		this.table = table;
		this.id = id;
		tid=0;
		t = tables.get(table);
		indexT = indexTables.get(table);
		partitionFinder = partitionFinders.get(table);
	}
	
	public CachingExecutor(String table, int id) {
		
		this.table = table;
		this.id = id;
		tid=0;
		t = tables.get(table);
		indexT = indexTables.get(table);
		partitionFinder = partitionFinders.get(table);
	}
	
	public void executeQuery(Query query, boolean cacheRequests, boolean cacheResults) {
		Op opQuery = Algebra.compile(query);	
        System.out.println(opQuery); 
        visitor = new OptimizeOpVisitorDPCachingStars(query, this, t,indexT,partitionFinder,cacheRequests,cacheResults);
        //visitor = new OptimizeOpVisitorDPCachingOld(query, this, t,indexT,partitionFinder,cacheRequests,cacheResults);
        OpWalker.walk(opQuery, visitor);

	}


	public String labelQuery(Query query, boolean cacheRequests, boolean cacheResults) {
		Op opQuery = Algebra.compile(query);	
        System.out.println(opQuery); 
        if(old)
        	visitor = new OptimizeOpVisitorLabelOld(query, this, t,indexT,partitionFinder,cacheRequests,cacheResults);
        else
        	visitor = new OptimizeOpVisitorLabelStars2(query, this, t,indexT,partitionFinder,cacheRequests,cacheResults);
        long start = System.currentTimeMillis();
        OpWalker.walk(opQuery, visitor);
        long stop = System.currentTimeMillis();
        double time = (stop-start);
        System.out.println("Exec time1: "+time+" ms");
		
        return "";
	}
	
	public static void connectTable(String table, Configuration conf) throws IOException {
		hconf = conf;
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			HTable indexTable=null;
			if(table.equals("L20k")){
				indexTable = new HTable( hconf, "L20_Index" );
			}
			else{
				indexTable = new HTable( hconf, table+"_Index" );
			}
			tables.put(table, t);
			indexTables.put(table, indexTable);
			partitionFinders.put(table, new PartitionFinder(t.getStartEndKeys()));
			StatisticsCache.initialize(t);
		}
	}
	
	public static HTable getTable(String table) throws IOException{
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			tables.put(table, t);
			return t;
		}
		else{
			return tables.get(table);
		}
	}

	public String getOutputFile() {
		return visitor.getOutputFile();
	}
	
	

}
