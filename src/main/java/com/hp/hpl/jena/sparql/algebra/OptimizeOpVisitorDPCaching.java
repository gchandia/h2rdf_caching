package com.hp.hpl.jena.sparql.algebra;

import fi.tkk.ics.jbliss.Digraph;
import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.dpplanner.CacheController;
import gr.ntua.h2rdf.dpplanner.CachedResult;
import gr.ntua.h2rdf.dpplanner.CachedResults;
import gr.ntua.h2rdf.dpplanner.CachingExecutor;
import gr.ntua.h2rdf.dpplanner.DPJoinPlan;
import gr.ntua.h2rdf.dpplanner.DPSolver;
import gr.ntua.h2rdf.dpplanner.DPSolverStars;
import gr.ntua.h2rdf.dpplanner.IndexScan;
import gr.ntua.h2rdf.dpplanner.Pair;
import gr.ntua.h2rdf.dpplanner.PowerSet;
import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.JoinExecutor;
import gr.ntua.h2rdf.indexScans.MergeJoinPlan;
import gr.ntua.h2rdf.indexScans.PartitionFinder;
import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess.Sys;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.function.library.max;
import com.hp.hpl.jena.sparql.syntax.PatternVars;

public class OptimizeOpVisitorDPCaching extends OpVisitorBase {
	public Query query;
	public HashMap<Integer, Var> varIds;
	public HashMap<Var, Integer> varRevIds;
	public HashMap<Integer, Triple> tripleIds;
	public HashMap<Integer, BGP> bgpIds;
	public HashMap<Triple, Integer> tripleRevIds;
	public TreeMap<Integer, BitSet> varGraphFull;
	public TreeMap<Integer, BitSet> varGraph;
	public TreeMap<Integer, BitSet> abstractVarGraph;
	public TreeMap<Integer, BitSet> skeletonVarGraph;
	public HashMap<Integer, Pair<Integer,BitSet>> starsVar, starsIDs;
	public HashMap<Integer, Integer> triplesStarRev;
	public TreeMap<Integer, TreeMap<Integer, BitSet>> edgeGraph;
	public TreeMap<Integer, TreeMap<Integer, BitSet>> abstractEdgeGraph;
	public TreeMap<Integer, TreeMap<Integer, BitSet>> skeletonEdgeGraph;
	public PartitionFinder partitionFinder;
	
	//public Digraph<Integer> digraph, digraph2;
	public int numTriples, numVars;
	public CachingExecutor cachingExecutor;
	public HTable indexTable;
	public HTable table;
	public long statsTime,numStats;
	public final long selectivityOffset=1;//00;
	public final int workers=50;
	protected List<ResultBGP> results;
	public boolean ordered, groupBy;
	public List<Var> orderVars, groupVars, projectionVars;
	public List<Integer> orderVarsInt;
	public HashMap<BitSet, Digraph<Integer>> digraphs;
	public BitSet selectiveIds, skeletonTriples, abstractTriples;
	public boolean cacheRequests,cacheResults;
	protected DPJoinPlan plan;
	
	public OptimizeOpVisitorDPCaching() {
		// TODO Auto-generated constructor stub
	}
	
	public OptimizeOpVisitorDPCaching(Query query, CachingExecutor cachingExecutor, HTable table, HTable indexTable, PartitionFinder partitionFinder, boolean cacheRequests, boolean cacheResults) {
		this.partitionFinder =partitionFinder;
		this.cacheResults = cacheResults;
		this.cacheRequests = cacheRequests;
		this.query=query;
		this.cachingExecutor = cachingExecutor;
		this.table=table;
		this.indexTable=indexTable;
		ordered=query.isOrdered();
		groupBy = query.hasGroupBy();
		projectionVars = query.getProjectVars();
		orderVars= new ArrayList<Var>();
		groupVars= new ArrayList<Var>();
		if(query.isOrdered()){
			Iterator<SortCondition> it = query.getOrderBy().iterator();
			while(it.hasNext()){
				SortCondition cond = it.next();
				orderVars.add(cond.getExpression().asVar());
				System.out.println("Order By:"+cond.getExpression().asVar());
			}
		}
		orderVarsInt = new ArrayList<Integer>();
		if(groupBy){
			groupVars = query.getGroupBy().getVars();
		}
	}
	
	public void visit(OpOrder opOrder)
    {
		/*try {
			List<Var> orderVars = new ArrayList<Var>();
			
			for(SortCondition cond : opOrder.getConditions()){
				orderVars.add(cond.getExpression().getExprVar().asVar());
			}
			Integer firstOrderVar = varRevIds.get(orderVars.get(0));
			long[][] maxPartition = null;
			int plength=0;
			BitSet bgps = varGraph.get(firstOrderVar);
			for (Integer k = bgps.nextSetBit(0); k >= 0; k = bgps.nextSetBit(k+1)) {
				BGP bgp = bgpIds.get(k);
				long[][] p;
				p = bgp.getPartitions(orderVars.get(0));
				if(p.length>plength){
					maxPartition=p;
					plength=p.length;
				}
			}
			double size=0.0;
			ResultBGP result = results.get(0);
			for(Var v : result.joinVars){
				double[] stats = result.getStatistics(v,this);
				if(stats[0]>size){
					size=stats[0];
				}
			}
			size*=result.joinVars.size();
			System.out.println(size);
			
			for (int i = 0; i < maxPartition.length; i++) {
				System.out.print(maxPartition[i][1]+" ");
			}
			System.out.println();
			ResultBGP res = null;
			if(size<=2000000)
				res = JoinExecutor.executeOrdering(results.get(0), maxPartition, true);
			else
				res = JoinExecutor.executeOrdering(results.get(0), maxPartition, false);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    }

	public void visit(OpBGP opBGP)
    {
		try {
			
			//partitionFinder = new PartitionFinder(table.getStartEndKeys());
			
			varIds = new HashMap<Integer, Var>();
			varRevIds = new HashMap<Var, Integer>();
			tripleIds = new HashMap<Integer, Triple>();
			bgpIds = new HashMap<Integer, BGP>();
			tripleRevIds = new HashMap<Triple, Integer>();
			varGraph = new TreeMap<Integer, BitSet>();
			varGraphFull = new TreeMap<Integer, BitSet>();
			edgeGraph = new TreeMap<Integer, TreeMap<Integer,BitSet>>();
			
	    	System.out.println(opBGP.toString());
	    	
	    	Set<Var> vars = PatternVars.vars(query.getQueryPattern());
	    	Iterator<Var> it = vars.iterator();
	    	Integer id =0;
	    	numVars=0;
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		varIds.put(id,v);
	    		varRevIds.put(v,id);
	    		id++;
	    		numVars++;
	    	}
	    	System.out.println(varIds);

			for(Var v: orderVars){
				orderVarsInt.add(varRevIds.get(v));
			}
			
	    	List<Triple> triples= opBGP.getPattern().getList();
			Iterator<Triple> it1 = triples.iterator();
	    	id =0;
	    	numTriples=0;
			while(it1.hasNext()){
				Triple t = it1.next();
    			BGP b = new BGP(t,table,indexTable, partitionFinder,this);
    			
    			//b.processSubclass();
    			bgpIds.put(id,b);
				tripleIds.put(id,t);
				tripleRevIds.put(t, id);
	    		id++;
	    		numTriples++;
			}
	    	System.out.println(tripleIds);
	    	vars = PatternVars.vars(query.getQueryPattern());
	    	it = vars.iterator();
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		BitSet l = new BitSet(numVars);
	    		triples= opBGP.getPattern().getList();
	    		it1 = triples.iterator();
	    		while(it1.hasNext()){
	    			Triple t =it1.next();
	    			if(t.getSubject().equals(v) || t.getPredicate().equals(v) || t.getObject().equals(v)){
	    				l.set(tripleRevIds.get(t));
	    			}
	    		}
	    		varGraphFull.put(varRevIds.get(v), l);
	    	}
	    	
	    	System.out.println(varGraphFull);

	    	starsVar = new HashMap<Integer, Pair<Integer,BitSet>>();
	    	starsIDs = new HashMap<Integer, Pair<Integer,BitSet>>();
	    	for(Entry<Integer,BitSet> e : varGraphFull.entrySet()){
	    		if(e.getValue().cardinality()>=3){
	    			System.out.println("Star var: "+e.getKey());
	    			Integer idstar =starsVar.size();
	    			Pair<Integer,BitSet> e1 = new Pair(idstar, e.getValue());
	    			starsVar.put(e.getKey(), e1);
	    			e1 = new Pair(e.getKey(), e.getValue());
	    			starsIDs.put(idstar, e1);
	    		}
	    	}
	    	System.out.println("Stars: "+starsIDs);
	    	
	    	//make skeleton graph
	    	skeletonTriples = new BitSet(numTriples);
	    	triples= opBGP.getPattern().getList();
			it1 = triples.iterator();
			while(it1.hasNext()){
				Triple t =it1.next();
				Integer tid=tripleRevIds.get(t);
				boolean foundStar =false;
				if(!foundStar && t.getSubject().isVariable()){
					Integer vid = varRevIds.get(t.getSubject());
					Pair<Integer, BitSet> star = starsVar.get(vid);
					if(star!=null){
						System.out.println(t);
						//belongs to a star check other variables cardinality
						boolean otherVarSkeleton = false;
						if(t.getPredicate().isVariable()){
							Integer id2 = varRevIds.get(t.getPredicate());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
						}
						if(t.getObject().isVariable()){
							Integer id2 = varRevIds.get(t.getObject());
							System.out.println("Var: "+id2);
							System.out.println("Size: "+varGraphFull.get(id2).cardinality());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
							System.out.println(otherVarSkeleton);
						}
						if(!otherVarSkeleton)
							foundStar = true;
					}
				}
				if(!foundStar && t.getPredicate().isVariable()){
					Integer vid = varRevIds.get(t.getPredicate());
					Pair<Integer, BitSet> star = starsVar.get(vid);
					if(star!=null){
						//belongs to a star check other variables cardinality
						boolean otherVarSkeleton =false;
						if(t.getSubject().isVariable()){
							Integer id2 = varRevIds.get(t.getSubject());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
						}
						if(t.getObject().isVariable()){
							Integer id2 = varRevIds.get(t.getObject());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
						}
						if(!otherVarSkeleton)
							foundStar = true;
					}
				}
				if(!foundStar && t.getObject().isVariable()){
					Integer vid = varRevIds.get(t.getObject());
					Pair<Integer, BitSet> star = starsVar.get(vid);
					if(star!=null){
						//belongs to a star check other variables cardinality
						boolean otherVarSkeleton = false;
						if(t.getSubject().isVariable()){
							Integer id2 = varRevIds.get(t.getSubject());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
						}
						if(t.getPredicate().isVariable()){
							Integer id2 = varRevIds.get(t.getPredicate());
							otherVarSkeleton = otherVarSkeleton || (varGraphFull.get(id2).cardinality()>=2);
						}
						
						if(!otherVarSkeleton)
							foundStar = true;
					}
				}
				System.out.println("Triple: "+tid+" foundStar: "+foundStar);
				if(!foundStar)
					skeletonTriples.set(tid);
			}
			System.out.println("Skeleton triples: "+skeletonTriples);
			
	    	for(Entry<Integer,BitSet> e : varGraphFull.entrySet()){
	    		BitSet edges = new BitSet(numTriples);
	    		edges.or(e.getValue());
	    		edges.and(skeletonTriples);
	    		if(edges.cardinality()>0)
	    			varGraph.put(e.getKey(), edges);
	    	}

	    	System.out.println(varGraph);
	    	
	    	//make edge graph
			triples= opBGP.getPattern().getList();
			it1 = triples.iterator();
			while(it1.hasNext()){
				Triple t =it1.next();
				Integer tid=tripleRevIds.get(t);
				if(skeletonTriples.get(tid)){
					TreeMap<Integer, BitSet> map = new TreeMap<Integer, BitSet>();
					if(t.getSubject().isVariable()){
						Integer vid = varRevIds.get(t.getSubject());
						BitSet l = varGraph.get(vid);
						BitSet bn = new BitSet(numTriples);
						bn.or(l);
						bn.clear(tid);
						map.put(vid, bn);
					}
					if(t.getPredicate().isVariable()){
						Integer vid = varRevIds.get(t.getPredicate());
						BitSet l = varGraph.get(vid);
						BitSet bn = new BitSet(numTriples);
						bn.or(l);
						bn.clear(tid);
						map.put(vid, bn);
					}
					if(t.getObject().isVariable()){
						Integer vid = varRevIds.get(t.getObject());
						BitSet l = varGraph.get(vid);
						BitSet bn = new BitSet(numTriples);
						bn.or(l);
						bn.clear(tid);
						map.put(vid, bn);
					}
					edgeGraph.put(tid, map);
				}
			}
	
	    	System.out.println(edgeGraph);
	    	
	    	selectiveIds = new BitSet(bgpIds.entrySet().size()*3);
	    	long max = 0;
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		int sid = e.getKey()*3;
	    		if(tr.getS()>=selectivityOffset){
	    			if(tr.getS()>max){
	    				max = tr.getS();
	    			}
	    		}
	    		if(tr.getO()>=selectivityOffset){
	    			if(tr.getO()>max){
	    				max = tr.getO();
	    			}
	    		}
	    	}
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		int sid = e.getKey()*3;
	    		if(tr.getS()>=selectivityOffset){
	    			if(max>10000 && tr.getS()>1000){
	    				selectiveIds.set(sid);
	    			}
	    			else if(max<800){
	    				selectiveIds.set(sid);
	    			}
	    		}
	    		//if(tr.getP()>=selectivityOffset){
	    		//	selectiveIds.set(sid+1);
	    		//}
	    		if(tr.getO()>=selectivityOffset){
	    			if(max>10000 && tr.getO()>1000){
	    				selectiveIds.set(sid+2);
	    			}
	    			else if(max<800){
	    				selectiveIds.set(sid+2);
	    			}
	    		}
	    	}
	    	System.out.println("SelectiveIds "+selectiveIds);
	    	digraphs = new HashMap<BitSet, Digraph<Integer>>();
	    	PowerSet ps =new PowerSet(selectiveIds);
	    	Iterator<BitSet> pit = ps.iterator();
	    	while(pit.hasNext()){
	    		BitSet bt = pit.next();
	    		//System.out.println(bt);
	    		Digraph<Integer> tempDigraph = createDigraph(bt);
	    		digraphs.put(bt, tempDigraph);
	    	}
	    	
	    	
	    	statsTime=0;
	    	numStats=0;
	    	DPSolverStars solver = new DPSolverStars();

	    	long start = System.nanoTime();
	    	plan = solver.solve(this,cachingExecutor);

	    	long stop = System.nanoTime();
	    	System.out.println("Solve time us: "+(stop-start)/1000);
	    	System.out.println("Number of stats requests: "+numStats);
	    	System.out.println("Total stats time us: "+(statsTime)/1000);
	    	System.out.println(plan.print());
	
	    	/*plan.execute(this,cachingExecutor);

			double maxcov = (float)solver.maxCoverage/(float)numTriples;
			if(cacheRequests)
				CacheController.finishQuery(plan.getCost(),maxcov, this);
	    	results = plan.getResults();*/
	    	ResultBGP res = new ResultBGP(vars, new Path("output/join1"), null);
	    	results = new ArrayList<ResultBGP>();
	    	results.add(res);
	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	
    }

	private Digraph<Integer> createDigraph(BitSet bt) {
		Digraph<Integer> ret = new Digraph<Integer>();
		TreeSet<String> colorSet = new TreeSet<String>();
    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
    		int ii = e.getKey()*3;
    		ByteTriple tr = e.getValue().byteTriples.get(0);
    		String sig = getSignature3(tr, bt, ii);
    		colorSet.add(sig);
    	}
    	//System.out.println(colorSet);
    	HashMap<String,Integer> vertexColors = new HashMap<String, Integer>();
    	int i1 =0;
    	for(String s : colorSet){
    		vertexColors.put(s, i1);
    		i1++;
    	}

    	//add vertices
    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
    		int ii = e.getKey()*3;
    		ByteTriple tr = e.getValue().byteTriples.get(0);
    		String sign = getSignature3(tr, bt, ii);
    		Integer colorId2 = vertexColors.get(sign);
    		ret.add_vertex(e.getKey(), colorId2);
    	}
    	int size = ret.nof_vertices();
    	//add 3 layers and connect them
    	int colors = vertexColors.size(); 
    	for (Integer i = 0; i < size; i++) {
			ret.add_vertex(size+i,ret.vertices.get(i).color+colors);
			ret.add_edge(i, size+i);
		}
    	int s2=size+size;
    	int c2=colors+colors;
    	for (Integer i = 0; i < size; i++) {
			ret.add_vertex(s2+i,ret.vertices.get(i).color+c2);
			ret.add_edge(size+i,s2+i);
		}
    	//add edges to the respective layers
    	for(Entry<Integer, TreeMap<Integer,BitSet>> e : edgeGraph.entrySet()){
    		int nodeId=e.getKey();
    		for (Entry<Integer,BitSet> edges:e.getValue().entrySet()) {
    			BitSet bs = edges.getValue();
				for (int dest = bs.nextSetBit(0); dest >= 0; dest = bs.nextSetBit(dest+1)) {
					String s =bgpIds.get(nodeId).varPos.get(varIds.get(edges.getKey()).toString());
					String d =bgpIds.get(dest).varPos.get(varIds.get(edges.getKey()).toString());
					if(s.compareTo(d)>=0){
						addEdge2(ret,nodeId, dest,s+""+d,size);
						//System.out.println("Edge:"+nodeId+" "+dest+" : "+s+d);
					}
				}
			}
    	}
    	//ret.write_dot(System.out);
		return ret;
	}

	private void addEdge2(Digraph<Integer> ret, int srcId, int destId, String type, int size) {
		int s2 = size+size;
		if(type.equals("ss")){//001
			ret.add_edge(srcId, destId);
		}
		else if(type.equals("sp")){//010
			ret.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("so")){//011
			ret.add_edge(srcId, destId);
			ret.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("pp")){//100
			ret.add_edge(srcId+s2, destId+s2);
		}
		else if(type.equals("po")){//101
			ret.add_edge(srcId+s2, destId+s2);
			ret.add_edge(srcId, destId);
		}
		else if(type.equals("oo")){//110
			ret.add_edge(srcId+s2, destId+s2);
			ret.add_edge(srcId+size, destId+size);
		}
		
	}

	private String getSignature3(ByteTriple tr, BitSet bt, int i) {
		String ret = "";
		if(bt.get(i)){
			ret+="0_";
		}
		else{
			ret+=tr.getS()+"_";
		}
		if(bt.get(i+1)){
			ret+="0_";
		}
		else{
			ret+=tr.getP()+"_";
		}
		if(bt.get(i+2)){
			ret+="0_";
		}
		else{
			ret+=tr.getO()+"_";
		}
		return ret;
	}

	private String getSignature2(ByteTriple tr) {
		String ret = "";
		if(tr.getS()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getS()+"_";
		}
		if(tr.getP()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getP()+"_";
		}
		if(tr.getO()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getO()+"_";
		}
		return ret;
	}

/*	private void addEdge(int srcId, int destId, String type, int size) {
		int s2 = size+size;
		if(type.equals("ss")){//001
			digraph.add_edge(srcId, destId);
			
			digraph2.add_edge(srcId, destId);
		}
		else if(type.equals("sp")){//010
			digraph.add_edge(srcId+size, destId+size);
			
			digraph2.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("so")){//011
			digraph.add_edge(srcId, destId);
			digraph.add_edge(srcId+size, destId+size);

			digraph2.add_edge(srcId, destId);
			digraph2.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("pp")){//100
			digraph.add_edge(srcId+s2, destId+s2);

			digraph2.add_edge(srcId+s2, destId+s2);
		}
		else if(type.equals("po")){//101
			digraph.add_edge(srcId+s2, destId+s2);
			digraph.add_edge(srcId, destId);

			digraph2.add_edge(srcId+s2, destId+s2);
			digraph2.add_edge(srcId, destId);
		}
		else if(type.equals("oo")){//110
			digraph.add_edge(srcId+s2, destId+s2);
			digraph.add_edge(srcId+size, destId+size);
			

			digraph2.add_edge(srcId+s2, destId+s2);
			digraph2.add_edge(srcId+size, destId+size);
		}
	}
	*/
	public String getOutputFile() {
		/*"output/join_"+cachingExecutor.id+"_"+(cachingExecutor.tid-1);
		if(plan.equals(CachedResult.class))
			return (((CachedResult)(results.get(0))).results.get(0).path)+"";*/
		return results.get(0).path.toString();
	}
	
}
