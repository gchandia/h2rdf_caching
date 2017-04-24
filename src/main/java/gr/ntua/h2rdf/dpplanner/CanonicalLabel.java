package gr.ntua.h2rdf.dpplanner;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

import fi.tkk.ics.jbliss.Digraph;
import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.ResultBGP;

public class CanonicalLabel {

/*	public static String getLabel(BitSet edges, OptimizeOpVisitorDPCaching visitor) {
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		return visitor.digraph.canonical_labeling1(edges, visitor,canonicalVarMapping);
	}

	public static CachedResults checkCache(BitSet edges,
			OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor) {
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		String label = visitor.digraph.canonical_labeling1(edges, visitor,canonicalVarMapping);
		CachedResults result = cachingExecutor.resultCache.get(label);
		if(result==null)
			return null;
		for(CachedResult r : result.results){
			r.clearTempData();
			r.currentCanonicalVariableMapping=canonicalVarMapping;
		}
		for(CachedResult cr :result.results){
			for(ResultBGP r : cr.results){
				r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
				for (int i = 0; i < canonicalVarMapping.size(); i++) {
					r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
				}
				cr.varRelabeling=r.varRelabeling;
			}
		}
		return result;
	}
*/
	public static void cache(BitSet edges, OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor, CachedResult result) {
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		String label = visitor.digraphs.get(new BitSet(visitor.bgpIds.entrySet().size()*3)).canonical_labeling1(edges, visitor,canonicalVarMapping);
		//System.out.println("LabeL: "+label);
		//System.out.println(canonicalVarMapping);
		if(label.equals("$1_113_$2&$1_176_$3&$1_298_$4&$1_301_$5&$1_407_$6&$1_85_$7&"))
			return;
		
		result.cachedCanonicalVariableMapping=canonicalVarMapping;
		
		CacheController.cache(label, result);
		
		//cachingExecutor.resultCache.put(label, stats);
	}
	public static void cacheStars(BitSet edges, OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor, CachedResult result) {
		System.out.println("Triples: "+edges);
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		BitSet selectiveIDs = new BitSet(visitor.bgpIds.entrySet().size()*3);
		Digraph<Integer> d = visitor.digraphs.get(selectiveIDs);
		//find skeleton triples
		BitSet edges2 = new BitSet();
		edges2.or(edges);
		boolean containsStars = false;
		for(Entry<Integer, Pair<Integer,BitSet>> e : visitor.starsIDs.entrySet()){
			if(edges2.get(e.getKey())){
				containsStars=true;
				edges2.clear(e.getKey());
				BitSet b = e.getValue().getSecond();
				for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i+1)) {
					if(visitor.skeletonTriples.get(i)){
						boolean isSkeleton=false;
						for(Entry<Integer, BitSet> e1 : visitor.abstractEdgeGraph.get(i).entrySet()){
							BitSet test =new BitSet();
							test.or(edges2);
							if(e1.getKey()==e.getValue().getFirst())
								test.andNot(b);//not star skeleton labels
							test.clear(e.getKey());//not star skeleton labels
							test.and(e1.getValue());
							if(test.cardinality()>0){
								isSkeleton=true;
								break;
							}
						}
						if(!isSkeleton)
							edges2.clear(i);
					}
				}
			}
		}
		System.out.println("Skeleton triples: "+edges2);
		HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
		String label="";
		if(edges2.cardinality()>0){
			List<Pair<Integer,TreeSet<String>>> starLabels = new ArrayList<Pair<Integer,TreeSet<String>>>();
			
			label = d.canonical_labelingStars2(selectiveIDs,edges, edges2, visitor, selectiveBindings, canonicalVarMapping, starLabels);
			System.out.println("Skeleton: "+label);
			if(!starLabels.isEmpty()){
				label+="!";
				for(Pair<Integer,TreeSet<String>> p: starLabels){
					label+="("+p.getFirst()+"|";
					for(String s : p.getSecond()){
						label+=s+"&";
					}
					label+=")";
				}
			}
		}
		else{//star only
	    	TreeSet<String> starLabels = d.canonical_labelingStarsNoSkeleton(selectiveIDs,edges, visitor, selectiveBindings, canonicalVarMapping);
			if(!starLabels.isEmpty()){
				label+="!(1|";
				for(String s: starLabels){
					label+=s+"&";
				}
				label+=")";
			}
		}
		System.out.println("Label: "+label);
		result.cachedCanonicalVariableMapping=canonicalVarMapping;
		
		CacheController.cache(label, result);
		
		//cachingExecutor.resultCache.put(label, stats);
	}
/*	public static CachedResults checkCacheNoSelective(BitSet edges,
			OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor) {
		HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		
		String label = visitor.digraph2.canonical_labelingNoSelective(edges, visitor, selectiveBindings, canonicalVarMapping);
		CachedResults result = cachingExecutor.resultCache.get(label);
		if(result==null)
			return null;
		for(CachedResult r : result.results){
			r.clearTempData();
			r.selectiveBindings=selectiveBindings;
			r.currentCanonicalVariableMapping=canonicalVarMapping;
		}
		for(CachedResult cr :result.results){
			for(ResultBGP r : cr.results){
				r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
				for (int i = 0; i < canonicalVarMapping.size(); i++) {
					r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
				}
				cr.varRelabeling=r.varRelabeling;
			}
		}
		return result;
	}*/


	public static CachedResults checkCache2(BitSet edges, OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor, Double computeCost) {

		CachedResults result = null;
    	BitSet selIds = new BitSet(visitor.numTriples);
    	for (int i = visitor.selectiveIds.nextSetBit(0); i >= 0; i = visitor.selectiveIds.nextSetBit(i+1)) {
    		if(edges.get(i/3)){
    			selIds.set(i);
    		}
    	}
    	PowerSet ps =new PowerSet(selIds);
    	Iterator<BitSet> pit = ps.iterator();
    	while(pit.hasNext()){
			HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
			TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
    		BitSet bs = pit.next();
    		Digraph<Integer> dig = visitor.digraphs.get(bs);
    		long start=0;
    		//if(bs.isEmpty())
    		//	start= System.nanoTime();
			String label = dig.canonical_labeling2(bs, edges, visitor, selectiveBindings, canonicalVarMapping);
    		//if(bs.isEmpty()){
    		//	long stop= System.nanoTime();
    		//	System.out.println("Labelling time for full query(us): "+ (stop-start));
    		//}
			System.out.println("Label: "+label);
			CachedResults res = CacheController.resultCache.get(label);
			if(res==null){
				//Not found make cache request
				BitSet joinOrder = new BitSet();
				/*for (int i = 0; i < visitor.numTriples; i++) {
					if(edges.get(i)){
						for(Integer v : visitor.edgeGraph.get(i).keySet()){
							BitSet nodes = (BitSet) visitor.edgeGraph.get(i).get(v).clone();
							nodes.andNot(edges);
							if(!nodes.isEmpty())
								joinOrder.set(v);
						}
					}
				}*/
				if(visitor.cacheRequests){
					//if(selectiveBindings.size()==0)
						CacheController.request(visitor, label,selectiveBindings, canonicalVarMapping, edges,computeCost,joinOrder);
				}
				continue;
			}
			//System.out.println(res.print());
			for(CachedResult r : res.results){
				r.clearTempData();
				r.selectiveBindings=selectiveBindings;
				r.currentCanonicalVariableMapping=canonicalVarMapping;
			}
			for(CachedResult cr :res.results){
				for(ResultBGP r : cr.results){
					r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
					for (int i = 0; i < canonicalVarMapping.size(); i++) {
						r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
					}
					cr.varRelabeling=r.varRelabeling;
				}
			}
			if(result==null){
				result = res;
			}
			else{
				result.results.addAll(res.results);
			}
    	}
		/*for( Entry<BitSet, Digraph<Integer>> e : visitor.digraphs.entrySet()){
			HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
			TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
			BitSet bs = e.getKey();
			//System.out.println("Checking: "+bs);
			Digraph<Integer> dig = e.getValue();
			String label = dig.canonical_labeling2(bs, edges, visitor, selectiveBindings, canonicalVarMapping);
			//System.out.println("Label: "+label);
			CachedResults res = CacheController.resultCache.get(label);
			if(res==null){
				//Not found make cache request
				CacheController.request(visitor, label,selectiveBindings, canonicalVarMapping, edges);
				continue;
			}
			//System.out.println(res.print());
			for(CachedResult r : res.results){
				r.clearTempData();
				r.selectiveBindings=selectiveBindings;
				r.currentCanonicalVariableMapping=canonicalVarMapping;
			}
			for(CachedResult cr :res.results){
				for(ResultBGP r : cr.results){
					r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
					for (int i = 0; i < canonicalVarMapping.size(); i++) {
						r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
					}
					cr.varRelabeling=r.varRelabeling;
				}
			}
			if(result==null){
				result = res;
			}
			else{
				result.results.addAll(res.results);
			}
		}*/
		return result;
	}
	
	public static CachedResults checkCacheStars(BitSet edges, OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor, Double computeCost) {
		//find skeleton triples
		BitSet edges2 = new BitSet();
		edges2.or(edges);
		boolean containsStars = false;
		for(Entry<Integer, Pair<Integer,BitSet>> e : visitor.starsIDs.entrySet()){
			if(edges2.get(e.getKey())){
				containsStars=true;
				edges2.clear(e.getKey());
				BitSet star = e.getValue().getSecond();
				for (int i = star.nextSetBit(0); i >= 0; i = star.nextSetBit(i+1)) {
					if(visitor.skeletonTriples.get(i)){
						edges2.set(i);
					}
				}
			}
		}
		BitSet edges3 = new BitSet();
		for (int i = edges2.nextSetBit(0); i >= 0; i = edges2.nextSetBit(i+1)) {
			BitSet neighbors = new BitSet();
			for(BitSet b : visitor.skeletonEdgeGraph.get(i).values()){
				neighbors.or(b);
			}
			neighbors.and(edges2);
			if(visitor.triplesStarRev.containsKey(i)){
				BitSet star = visitor.starsIDs.get(visitor.triplesStarRev.get(i)).getSecond();
				neighbors.andNot(star);
				if(neighbors.cardinality()>0)
					edges3.set(i);
			}
			else{
				edges3.set(i);
			}
		}
		
		
		System.out.println("Skeleton triples: "+edges3);
		
		CachedResults result = null;

		if(edges3.cardinality()>0){
	    	BitSet selIds = new BitSet(visitor.numTriples);
	    	for (int i = visitor.selectiveIds.nextSetBit(0); i >= 0; i = visitor.selectiveIds.nextSetBit(i+1)) {
	    		if(edges3.get(i/3)){
	    			selIds.set(i);
	    		}
	    	}
	    	PowerSet ps =new PowerSet(selIds);
	    	Iterator<BitSet> pit = ps.iterator();
	    	while(pit.hasNext()){
				HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
				TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
	    		BitSet bs = pit.next();
	    		Digraph<Integer> dig = visitor.digraphs.get(bs);
	    		long start=0;
	    		//if(bs.isEmpty())
	    		//	start= System.nanoTime();
				String label = dig.canonical_labelingStars(bs, edges3, visitor, selectiveBindings, canonicalVarMapping);
	    		//if(bs.isEmpty()){
	    		//	long stop= System.nanoTime();
	    		//	System.out.println("Labelling time for full query(us): "+ (stop-start));
	    		//}
				//System.out.println("label: "+label);
				String skeleton="", stars="";
				if(containsStars){
					skeleton = label.substring(0,label.indexOf("{"));
					stars = label.substring(label.indexOf("{"));
				}
				else{
					skeleton = label;
				}
				System.out.println("Skeleton label: "+skeleton+ " Star label: "+stars);
				SortedMap<String, CachedResults> res = CacheController.resultCache.subMap(skeleton, skeleton);
				if(res==null){
					for (CachedResults r : res.values()) {
						System.out.println(r.print());
						
					}
					//Not found make cache request
					//BitSet joinOrder = new BitSet();
					/*for (int i = 0; i < visitor.numTriples; i++) {
						if(edges.get(i)){
							for(Integer v : visitor.edgeGraph.get(i).keySet()){
								BitSet nodes = (BitSet) visitor.edgeGraph.get(i).get(v).clone();
								nodes.andNot(edges);
								if(!nodes.isEmpty())
									joinOrder.set(v);
							}
						}
					}*/
					
					//continue;
				}
				/*//System.out.println(res.print());
				for(CachedResult r : res.results){
					r.clearTempData();
					r.selectiveBindings=selectiveBindings;
					r.currentCanonicalVariableMapping=canonicalVarMapping;
				}
				for(CachedResult cr :res.results){
					for(ResultBGP r : cr.results){
						r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
						for (int i = 0; i < canonicalVarMapping.size(); i++) {
							r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
						}
						cr.varRelabeling=r.varRelabeling;
					}
				}
				if(result==null){
					result = res;
				}
				else{
					result.results.addAll(res.results);
				}*/
	    	}
		}
		return result;
	}
	public static String label(OptimizeOpVisitorDPCaching visitor){

		HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
		TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
		BitSet bs = new BitSet(visitor.numTriples);
		BitSet edges = new BitSet(visitor.numTriples);

		for (int i = 0; i < visitor.numTriples; i++) {
			edges.set(i);
		}
		for(BitSet b : visitor.digraphs.keySet()){
			System.out.println(b);
		}
		Digraph<Integer> dig = visitor.digraphs.get(bs);
		String label = dig.canonical_labeling2(bs, edges, visitor, selectiveBindings, canonicalVarMapping);
		System.out.println(label);
		return label;
	}
	
	public static String lableStars2(OptimizeOpVisitorDPCaching visitor) {
		//find skeleton triples
		int n=visitor.numTriples+visitor.starsVar.size();
		BitSet edges = new BitSet(n);
		edges.or(visitor.abstractTriples);
		BitSet edges2 = new BitSet();
		edges2.or(visitor.skeletonTriples);
		System.out.println("Skeleton triples: "+edges2);
		

		if(edges2.cardinality()>0){
				HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
				TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
	    		BitSet bs = new BitSet(visitor.numTriples);
	    		Digraph<Integer> dig = visitor.digraphs.get(bs);
	    		long start=0;
	    		//if(bs.isEmpty())
	    		//	start= System.nanoTime();
	    		List<Pair<Integer,TreeSet<String>>> starLabels = new ArrayList<Pair<Integer,TreeSet<String>>>();
				String skeleton = dig.canonical_labelingStars2(bs,edges, edges2, visitor, selectiveBindings, canonicalVarMapping, starLabels);
	    		//if(bs.isEmpty()){
	    		//	long stop= System.nanoTime();
	    		//	System.out.println("Labelling time for full query(us): "+ (stop-start));
	    		//}
				System.out.println("Skeleton label: "+skeleton+ " Star label: "+starLabels);
		}
		else{//star only
				HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
				TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
	    		BitSet bs = new BitSet(visitor.numTriples);
	    		Digraph<Integer> dig = visitor.digraphs.get(bs);
	    		TreeSet<String> starLabels = dig.canonical_labelingStarsNoSkeleton(bs,edges, visitor, selectiveBindings, canonicalVarMapping);
	    		System.out.println(starLabels);
		}
		return "";
	}

	public static List<DPJoinPlan> checkCacheStars2(BitSet edges, OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor, Double computeCost) {
		//find skeleton triples
		BitSet edges2 = new BitSet();
		edges2.or(edges);
		boolean containsStars = false;
		for(Entry<Integer, Pair<Integer,BitSet>> e : visitor.starsIDs.entrySet()){
			if(edges2.get(e.getKey())){
				containsStars=true;
				edges2.clear(e.getKey());
				BitSet b = e.getValue().getSecond();
				for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i+1)) {
					if(visitor.skeletonTriples.get(i)){
						boolean isSkeleton=false;
						for(Entry<Integer, BitSet> e1 : visitor.abstractEdgeGraph.get(i).entrySet()){
							BitSet test =new BitSet();
							test.or(edges2);
							if(e1.getKey()==e.getValue().getFirst())
								test.andNot(b);//not star skeleton labels
							test.clear(e.getKey());//not star skeleton labels
							test.and(e1.getValue());
							if(test.cardinality()>0){
								isSkeleton=true;
								break;
							}
						}
						if(!isSkeleton)
							edges2.clear(i);
					}
				}
			}
		}
		System.out.println("Skeleton triples: "+edges2);
		
		List<DPJoinPlan> result = new ArrayList<DPJoinPlan>();

    	BitSet selIds = new BitSet(visitor.numTriples);
    	for (int i = visitor.selectiveIds.nextSetBit(0); i >= 0; i = visitor.selectiveIds.nextSetBit(i+1)) {
    		if(edges2.get(i/3)){
    			selIds.set(i);
    		}
    	}
		if(edges2.cardinality()>0){
	    	PowerSet ps =new PowerSet(selIds);
	    	Iterator<BitSet> pit = ps.iterator();
	    	while(pit.hasNext()){
				HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
				TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
	    		BitSet bs = pit.next();
	    		Digraph<Integer> dig = visitor.digraphs.get(bs);
	    		long start=0;
	    		//if(bs.isEmpty())
	    		//	start= System.nanoTime();
	    		List<Pair<Integer,TreeSet<String>>> starLabels = new ArrayList<Pair<Integer,TreeSet<String>>>();
				String skeleton = dig.canonical_labelingStars2(bs,edges, edges2, visitor, selectiveBindings, canonicalVarMapping, starLabels);
	    		//if(bs.isEmpty()){
	    		//	long stop= System.nanoTime();
	    		//	System.out.println("Labelling time for full query(us): "+ (stop-start));
	    		//}
				System.out.println("Skeleton label: "+skeleton+ " Star label: "+starLabels);
				SortedMap<String, CachedResults> res = CacheController.resultCache.subMap(skeleton, skeleton+"#");
				if(res!=null){
					for (Entry<String,CachedResults> r : res.entrySet()) {
						//System.out.println("Found: "+r.getKey());
						DPJoinPlan r1 = checkStarMatch(r,starLabels,selectiveBindings,canonicalVarMapping);
						if(r1!=null)
							result.add(r1);
					}
					//Not found make cache request
					//BitSet joinOrder = new BitSet();
					/*for (int i = 0; i < visitor.numTriples; i++) {
						if(edges.get(i)){
							for(Integer v : visitor.edgeGraph.get(i).keySet()){
								BitSet nodes = (BitSet) visitor.edgeGraph.get(i).get(v).clone();
								nodes.andNot(edges);
								if(!nodes.isEmpty())
									joinOrder.set(v);
							}
						}
					}*/
					
					//continue;
				}
				/*//System.out.println(res.print());
				for(CachedResult r : res.results){
					r.clearTempData();
					r.selectiveBindings=selectiveBindings;
					r.currentCanonicalVariableMapping=canonicalVarMapping;
				}
				for(CachedResult cr :res.results){
					for(ResultBGP r : cr.results){
						r.varRelabeling = new HashMap<Integer, Integer>(canonicalVarMapping.size());
						for (int i = 0; i < canonicalVarMapping.size(); i++) {
							r.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(i+1), canonicalVarMapping.get(i+1));
						}
						cr.varRelabeling=r.varRelabeling;
					}
				}
				if(result==null){
					result = res;
				}
				else{
					result.results.addAll(res.results);
				}*/
	    	}
		}
		else{//star only
	    	PowerSet ps =new PowerSet(selIds);
	    	Iterator<BitSet> pit = ps.iterator();
	    	while(pit.hasNext()){
				HashMap<Integer, Long> selectiveBindings= new HashMap<Integer, Long>();
				TreeMap<Integer,Integer> canonicalVarMapping= new TreeMap<Integer,Integer>();
	    		BitSet bs = pit.next();
	    		Digraph<Integer> dig = visitor.digraphs.get(bs);
	    		TreeSet<String> starLabels = dig.canonical_labelingStarsNoSkeleton(bs,edges, visitor, selectiveBindings, canonicalVarMapping);
	    		System.out.println(starLabels);
				//System.out.println("Canonical Var mapping: "+canonicalVarMapping);
	    		List<Pair<Integer,TreeSet<String>>> stLabels = new ArrayList<Pair<Integer,TreeSet<String>>>();
	    		Pair<Integer,TreeSet<String>> p = new Pair<Integer, TreeSet<String>>(1, starLabels);
	    		stLabels.add(p);
				SortedMap<String, CachedResults> res = CacheController.resultCache.subMap("", "#");
				if(res!=null){
					for (Entry<String,CachedResults> r : res.entrySet()) {
						DPJoinPlan r1 = checkStarMatch(r,stLabels,selectiveBindings,canonicalVarMapping);
						if(r1!=null)
							result.add(r1);
					}
				}
	    	}
		}
		return result;
	}
	
	
	private static DPJoinPlan checkStarMatch(Entry<String, CachedResults> r,
			List<Pair<Integer,TreeSet<String>>> starLabels, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping) {
		
		DPJoinPlan ret = null;
		String label = r.getKey();
		String skeleton="", stars="";
		skeleton = label.substring(0,label.indexOf("!"));
		stars = label.substring(label.indexOf("!")+1);
		StringTokenizer tok1 = new StringTokenizer(stars);
		int i=0, found=0;
		System.out.println("Checking: "+stars);

		TreeMap<Integer, Integer> newCanonicalVarMappingTemp = new TreeMap<Integer, Integer>();
		for(Entry<Integer,Integer> e : canonicalVarMapping.entrySet()){
			newCanonicalVarMappingTemp.put(e.getKey(), e.getValue());
		}
		while(tok1.hasMoreTokens() && (i<starLabels.size())){
			String st = tok1.nextToken(")");
			st = st.substring(1);
			System.out.println("Star: "+st);

			Integer var = Integer.parseInt(st.substring(0,st.indexOf("|")));
			if(starLabels.get(i).getFirst()==var){
				st = st.substring(st.indexOf("|")+1);
				StringTokenizer tok = new StringTokenizer(st);
				boolean match = false;
				Iterator<String> it = starLabels.get(i).getSecond().iterator();
				while(tok.hasMoreTokens()){
					String triple = tok.nextToken("&");
					//System.out.println(triple);
					match = false;
					while(it.hasNext()){
						String s = it.next();
						if(checkTripleLabel(triple,s,var,canonicalVarMapping, newCanonicalVarMappingTemp)){
							match=true;
							break;
						}
						else{//append join
							System.out.println("Join with: "+s);
						}
					}
					if(!match)
						break;
				}
				if(match){
					while(it.hasNext()){
						String s = it.next();
						System.out.println("Join with: "+s);
					}
					found++;
				}
			}
			i++;
		}
		if(found==i && !tok1.hasMoreTokens()){
			System.out.println("Match!!");
			//System.out.println("Canonical Var mapping: "+newCanonicalVarMappingTemp);

			for(CachedResult r1 : r.getValue().results){
				r1.clearTempData();
				r1.selectiveBindings=selectiveBindings;
				r1.currentCanonicalVariableMapping=newCanonicalVarMappingTemp;
			}
			for(CachedResult cr :r.getValue().results){
				//System.out.println("Cached Canonical Var mapping: "+cr.cachedCanonicalVariableMapping);
				for(ResultBGP r1 : cr.results){
					r1.varRelabeling = new HashMap<Integer, Integer>(newCanonicalVarMappingTemp.size());
					for (int ii = 0; ii < newCanonicalVarMappingTemp.size(); ii++) {
						if(newCanonicalVarMappingTemp.containsKey(ii+1) && cr.cachedCanonicalVariableMapping.containsKey(ii+1)){
							r1.varRelabeling.put(cr.cachedCanonicalVariableMapping.get(ii+1), newCanonicalVarMappingTemp.get(ii+1));
						}
					}
					cr.varRelabeling=r1.varRelabeling;
				}
				//System.out.println("Relabelling: "+cr.varRelabeling);
			}
			ret = r.getValue();
			return ret;
		}
		else{
			return null;
		}
	}
	
	private static boolean checkTripleLabel(String s1, String s2, Integer starVar, TreeMap<Integer, Integer> canonicalVarMapping, TreeMap<Integer, Integer> newCanonicalVarMapping) {
		//System.out.println("Checking: "+s1+" "+s2+" var: "+starVar);
		//s1 cached label
		//s2 current label
		String[] st1 =s1.split("_");
		String[] st2 =s2.split("_");
		int size = newCanonicalVarMapping.size()+1;
		//System.out.println("Canonical Var mapping: "+canonicalVarMapping);
		boolean ret = true;
		for (int i = 0; i < st2.length; i++) {
			if(st1[i].startsWith("$") && st2[i].startsWith("$")){//both var
				if(st1[i].equals("$"+starVar) ^ st2[i].equals("$"+starVar)){//one star var
					ret=false;
					break;
				}
				else{
					if(!st1[i].equals(st2[i])){//change canonical var mapping
						Integer i1 = Integer.parseInt(st1[i].substring(1));
						Integer i2 = Integer.parseInt(st2[i].substring(1));
						//System.out.println(i1+" "+i2);
						//move from i1->size and i2->i1
						Integer v2 = newCanonicalVarMapping.remove(i2);
						Integer v1 = newCanonicalVarMapping.remove(i1);
						//System.out.println("V: "+v1+" "+v2);
						newCanonicalVarMapping.put(size,v1);
						size++;
						newCanonicalVarMapping.put(i1,v2);
					}
				}
			}
			else if(st1[i].startsWith("$") ^ st2[i].startsWith("$")){//one var
				ret=false;
				break;
			}
			else{//both bound
				ret = st1[i].equals(st2[i]);
				if(!ret)
					break;
			}
		}
		if(ret){
			
		}
		//System.out.println("New Canonical Var mapping: "+newCanonicalVarMapping);
		//System.out.println(ret);
		return ret;
	}
	
	public static void main(String[] args) {
		TreeSet<String> s = new TreeSet<String>();
		s.add("$1_174_$2&$1_220_$3&$3_45_$4&$2_47_$5&$6_47_$5&$4_47_$6&!(1|$1_35_7454$1_89_7454)(4|$4_100_7454$4_41_$7$4_47_$10$4_47_$11$4_47_$12$4_47_$13$4_47_$14$4_47_$8$4_47_$9)");
		String s1 = "$1_174_$2&$1_220_$3&$3_45_$4&$2_47_$5&$6_47_$5&";//$4_47_$6&";
		System.out.println(s.subSet(s1, s1+"#"));
	}
}
