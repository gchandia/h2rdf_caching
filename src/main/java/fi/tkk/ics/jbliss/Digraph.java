/* 
 * @(#)Graph.java
 *
 * Copyright 2007-2010 by Tommi Junttila.
 * Released under the GNU General Public License version 3.
 */

package fi.tkk.ics.jbliss;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.dpplanner.CachedResult;
import gr.ntua.h2rdf.dpplanner.Pair;

import java.io.PrintStream;
import java.util.*;
import java.util.Map.Entry;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

/**
 * An undirected graph.
 * Vertices can be colored (with integers) and self-loops are allowed
 * but multiple edges between vertices are ignored.
 *
 * @author Tommi Junttila
 */
public class Digraph<V extends Comparable>
    implements Comparable
{
    /* Intermediate translator stuff for mapping
       bliss automorphisms back to jbliss automorphisms */
    protected Map<V,Integer> _bliss_map;
    protected Map<Integer,V> _bliss_map_inv;
    protected Reporter       _reporter;
    protected Object         _reporter_param;
    protected void _report(int[] aut)
    {
	if(_reporter == null)
	    return;
	Map<V,V> real_aut = new TreeMap<V,V>();
	for(Map.Entry<V,Integer> e : _bliss_map.entrySet())
	    real_aut.put(e.getKey(), _bliss_map_inv.get(aut[e.getValue()]));
	_reporter.report(real_aut, _reporter_param);
    }

    /* The internal JNI interface to true bliss */
    private native long create();
    private native void destroy(long true_bliss);
    protected native int _add_vertex(long true_bliss, int color);
    protected native void _add_edge(long true_bliss, int v1, int v2);
    protected native void _find_automorphisms(long true_bliss, Reporter r);
    protected native int[] _canonical_labeling(long true_bliss, Reporter r);
    //protected static native long _read_dimacs(String s);
    //protected native void _write_dimacs(long true_bliss, String s);
    //protected native long _permute(long true_bliss, int[] perm);

    public class Vertex implements Comparable
    {
	public V id;
	public int color;
	public TreeSet<Vertex> edges;
	protected void init(V identity, int c) {
	    assert c > 0;
	    id = identity;
	    color = c;
	    edges = new TreeSet<Vertex>();
	}
	public Vertex(V identity) {init(identity, 0); }
	public Vertex(V identity, int c) {assert c > 0; init(identity, c); }
	public int compareTo(Object other) throws ClassCastException {
	    if(getClass() != other.getClass())
		throw new ClassCastException("Internal error");
	    return id.compareTo(((Vertex)other).id);
	}
    }
    public Map<V, Vertex> vertices;


    /**
     * Create a new directed graph with no vertices or edges.
     */
    public Digraph()
    {
	vertices = new TreeMap<V, Vertex>();
	assert vertices != null;
    }


    /**
     * @return  the number of vertices in the graph
     */
    public int nof_vertices() {return vertices.size(); }

    
    /**
     * Output the graph in the graphviz dot format.
     *
     * @param stream  the output stream
     */
    public void write_dot(PrintStream stream)
    {
	stream.println("graph G {");
	for(Map.Entry<V, Vertex> e : vertices.entrySet()) {
	    V v = e.getKey();
	    Vertex vertex = e.getValue();
	    stream.print(v+"(c:"+vertex.color+"): ");
	    //stream.println("v"+v+" [label="+vertex.color+"];");
	    for(Vertex vertex2 : vertex.edges) {
	    	stream.print(vertex2.id+" ");
		//if(v.compareTo(vertex2.id) <= 0) {
		//    stream.println("v"+vertex.id+" -- v"+vertex2.id);
		//}
	    }
	    stream.println();
	}
	stream.println("}");
    }



   /**
     * The ordering between graphs
     */
    public int compareTo(Object other) throws ClassCastException {
	if(getClass() != other.getClass())
	    throw new ClassCastException("Cannot compare "+getClass()+
					 " to "+other.getClass());
	Digraph<V> h = (Digraph<V>)other;
	if(nof_vertices() < h.nof_vertices()) return -1;
	if(nof_vertices() > h.nof_vertices()) return 1;
	Iterator i1 = vertices.keySet().iterator();
	Iterator i2 = h.vertices.keySet().iterator();
	while(i1.hasNext())
	    {
		V name1 = (V)i1.next();
		V name2 = (V)i2.next();
		int i = name1.compareTo(name2);
		if(i < 0) return -1;
		if(i > 0) return 1;
		Vertex v1 = vertices.get(name1);
		Vertex v2 = h.vertices.get(name2);
		if(v1.color < v2.color) return -1;
		if(v1.color > v2.color) return 1;
		TreeSet<Vertex> edges1 = v1.edges;
		TreeSet<Vertex> edges2 = v2.edges;
		if(edges1.size() < edges2.size()) return -1;
		if(edges1.size() > edges2.size()) return 1;
		Iterator vi1 = edges1.iterator();
		Iterator vi2 = edges2.iterator();
		while(vi1.hasNext()) {
		    int c = ((Vertex)vi1.next()).compareTo(vi2.next());
		    if(c < 0) return -1;
		    if(c > 0) return 1;
		}
	    }
	return 0;
    }




    /**
     * Add a new vertex (with the default color 0) into the graph.
     *
     * @param vertex  the vertex indentifier
     * @return  true if the vertex was not already in the graph
     */
    public boolean add_vertex(V vertex) {
	return add_vertex(vertex, 0);
    }


    /**
     * Add a new vertex into the graph.
     *
     * @param v       the vertex indentifier
     * @param color   the color of the vertex (a non-negative integer)
     * @return  true if the vertex was not already in the graph
     */
    public boolean add_vertex(V v, int color) {
	assert color >= 0;
	if(vertices.containsKey(v)) return false;
	vertices.put(v, new Vertex(v, color));
	return true;
    }


    /**
     * Delete a vertex from the graph.
     *
     * @param v       the vertex indentifier
     * @return  true if the vertex was in the graph
     */
    public boolean del_vertex(V v) {
	Vertex vertex = vertices.get(v);
	if(vertex == null) return false;
	for(Vertex vertex2 : vertex.edges)
	    vertex2.edges.remove(vertex);
	vertices.remove(v);
	return true;
    }


    /**
     * Add an undirected edge between the vertices v1 and v2.
     * If either of the vertices is not in the graph, it will be added.
     * Duplicate edges between vertices are ignored.
     *
     * @param v1  a vertex in the graph
     * @param v2  a vertex in the graph
     */
    public void add_edge(V v1, V v2) {
	Vertex vertex1 = vertices.get(v1);
	if(vertex1 == null) {
	    vertex1 = new Vertex(v1);
	    vertices.put(v1, vertex1);
	}
	Vertex vertex2 = vertices.get(v2);
	if(vertex2 == null) {
	    vertex2 = new Vertex(v2);
	    vertices.put(v2, vertex2);
	}
	vertex1.edges.add(vertex2);
    }


    /**
     * Remove an undirected edge between the vertices v1 and v2.
     *
     * @param v1  a vertex in the graph
     * @param v2  a vertex in the graph
     */
    public void del_edge(V v1, V v2) {
	Vertex vertex1 = vertices.get(v1);
	if(vertex1 == null)
	    return;
	Vertex vertex2 = vertices.get(v2);
	if(vertex2 == null)
	    return;
	vertex1.edges.remove(vertex2);
    }


    /**
     * Copy the graph.
     *
     * @return a copy of the graph
     */
    public Graph<V> copy()
    {
	Graph<V> g2 = new Graph<V>();
	for(Map.Entry<V,Vertex> e : vertices.entrySet())
	    g2.add_vertex(e.getKey(), e.getValue().color);
	for(Map.Entry<V,Vertex> e : vertices.entrySet())
	    for(Vertex vertex2 : e.getValue().edges)
		if(e.getValue().compareTo(vertex2) <= 0)
		    g2.add_edge(e.getKey(), vertex2.id);
 	return g2;
    }


    /**
     * Find (a generating set for) the automorphism group of the graph.
     * If the argument reporter is non-null,
     * then a generating set of automorphisms is reported by calling its
     * {@link Reporter#report} method for each generator.
     *
     * @param reporter        An object implementing the Reporter interface
     * @param reporter_param  The parameter passed to the Reporter object
     */
    public void find_automorphisms(Reporter reporter, Object reporter_param)
    {
        long bliss = create();
        assert bliss != 0;
	_bliss_map     = new TreeMap<V,Integer>();
	_bliss_map_inv = new TreeMap<Integer,V>();
	for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
	    V v = e.getKey();
	    Vertex vertex = e.getValue();
	    int bliss_vertex = _add_vertex(bliss, vertex.color);
	    _bliss_map.put(v, bliss_vertex);
	    _bliss_map_inv.put(bliss_vertex,v);
	}
	for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
	    V v = e.getKey();
	    Vertex vertex = e.getValue();
	    for(Vertex vertex2 : vertex.edges) {
		if(v.compareTo(vertex2.id) <= 0)
		    _add_edge(bliss,
			      _bliss_map.get(vertex.id),
			      _bliss_map.get(vertex2.id));
	    }
	}
	_reporter = reporter;
	_reporter_param = reporter_param;
        _find_automorphisms(bliss, _reporter);
	destroy(bliss);
	_bliss_map = null;
	_bliss_map_inv = null;
	_reporter = null;
	_reporter_param = null;
    }


    /**
     * Find the canonical labeling and the automorphism group of the graph.
     * If the argument reporter is non-null,
     * then a generating set of automorphisms is reported by calling its
     * {@link Reporter#report} method for each generator.
     *
     * @return           A canonical labeling permutation
     */
    public Map<V,Integer> canonical_labeling() {
	return canonical_labeling(null, null);
    }

    /**
     * Find the canonical labeling and the automorphism group of the graph.
     * If the argument reporter is non-null,
     * then a generating set of automorphisms is reported by calling its
     * {@link Reporter#report} method for each generator.
     *
     * @param reporter        An object implementing the Reporter interface
     * @param reporter_param  The parameter passed to the Reporter object
     * @return           A canonical labeling permutation
     */
    public Map<V,Integer> canonical_labeling(Reporter reporter,
					     Object reporter_param)
    {
        long bliss = create();
        assert bliss != 0;
	_bliss_map     = new TreeMap<V,Integer>();
	_bliss_map_inv = new TreeMap<Integer,V>();
	for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
	    V v = e.getKey();
	    Vertex vertex = e.getValue();
	    int bliss_vertex = _add_vertex(bliss, vertex.color);
	    _bliss_map.put(v, bliss_vertex);
	    _bliss_map_inv.put(bliss_vertex,v);
	}
	for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
	    V v = e.getKey();
	    Vertex vertex = e.getValue();
	    for(Vertex vertex2 : vertex.edges) {
		//if(v.compareTo(vertex2.id) <= 0)
		    _add_edge(bliss,
			      _bliss_map.get(vertex.id),
			      _bliss_map.get(vertex2.id));
	    }
	}
	_reporter = reporter;
	_reporter_param = reporter_param;
        int[] cf = _canonical_labeling(bliss, _reporter);
	destroy(bliss);
	TreeMap<V,Integer> labeling = new TreeMap<V,Integer>();
	for(Map.Entry<V,Integer> e : _bliss_map.entrySet())
	    labeling.put(e.getKey(), cf[e.getValue()]);
	_bliss_map = null;
	_bliss_map_inv = null;
	_reporter = null;
	_reporter_param = null;
	return labeling;
    }


    /**
     * Copy and relabel the graph.
     * The labeling is a Map that associates each vertex in the graph
     * into new vertex.
     *
     * @param labeling  the labeling to apply
     * @return the relabeled graph
     */
    public <W extends Comparable> Digraph<W> relabel(Map<V,W> labeling)
    {
	assert labeling != null;
	Digraph<W> g2 = new Digraph<W>();
	for(Map.Entry<V, Vertex> e : vertices.entrySet())
	    g2.add_vertex(labeling.get(e.getKey()), e.getValue().color);
	for(Map.Entry<V, Vertex> e : vertices.entrySet())
	    for(Vertex vertex2 : e.getValue().edges)
		    g2.add_edge(labeling.get(e.getKey()),
				labeling.get(vertex2.id));
 	return g2;
    }



    static {
	/* Load the C++ library including the true bliss and
	 * the JNI interface code */
	System.loadLibrary("jbliss");
    }



	public String canonical_labeling1(BitSet selectedVertices, 
			OptimizeOpVisitorDPCaching visitor, TreeMap<Integer, Integer> canonicalVarMapping) {
		int size = visitor.bgpIds.size();
        long bliss = create();
        assert bliss != 0;
		_bliss_map     = new TreeMap<V,Integer>();
		_bliss_map_inv = new TreeMap<Integer,V>();
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    int id = (Integer) v;
		    if(id>=size){
		    	id-=size;
		    }
		    if(id>=size){
		    	id-=size;
		    }
		    if(selectedVertices.get(id)){
			    Vertex vertex = e.getValue();
			    int bliss_vertex = _add_vertex(bliss, vertex.color);
			    _bliss_map.put(v, bliss_vertex);
			    _bliss_map_inv.put(bliss_vertex,v);
		    }
		}
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    Vertex vertex = e.getValue();
		    for(Vertex vertex2 : vertex.edges) {
		    	if(_bliss_map.containsKey(vertex.id) && _bliss_map.containsKey(vertex2.id)){
		    		   _add_edge(bliss,
		    				_bliss_map.get(vertex.id),
		    				_bliss_map.get(vertex2.id));
		    	}
		    }
		}
		_reporter = null;
		_reporter_param = null;
	        int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);
		TreeMap<Integer,Integer> labeling = new TreeMap<Integer,Integer>();
		String label="";
		
		for(Map.Entry<V,Integer> e : _bliss_map.entrySet()){
		    labeling.put(cf[e.getValue()],(Integer)e.getKey());
		}
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		for(Integer i:labeling.values()){
			if(selectedVertices.get(i)){
				ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
				Triple tr = visitor.tripleIds.get(i);
				if(btr.getS()>0){
					label+=btr.getS()+"_";
				}
				else{
					Integer varId = varlabels.get((Var)tr.getSubject());
					if(varId==null){
						varId = varlabels.size()+1;
						varlabels.put((Var)tr.getSubject(), varId);
					}
					label+="$"+varId+"_";
				}
				if(btr.getP()>0){
					label+=btr.getP()+"_";
				}
				else{
					Integer varId = varlabels.get((Var)tr.getPredicate());
					if(varId==null){
						varId = varlabels.size()+1;
						varlabels.put((Var)tr.getPredicate(), varId);
					}
					label+="$"+varId+"_";
				}
				if(btr.getO()>0)
					label+=btr.getO()+"&";
				else{
					//System.out.println(btr.getO());
					//System.out.println(tr.getObject());
					Integer varId = varlabels.get((Var)tr.getObject());
					if(varId==null){
						varId = varlabels.size()+1;
						varlabels.put((Var)tr.getObject(), varId);
					}
					label+="$"+varId+"&";
				}
				
				//label+=btr.getS()+"_"+btr.getP()+"_"+btr.getO()+"&";
			}
		}
		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		_bliss_map = null;
		_bliss_map_inv = null;
		_reporter = null;
		_reporter_param = null;
		//System.out.println("Label:"+label);
		return label;
	}

	public String canonical_labelingNoSelective(BitSet selectedVertices,
			OptimizeOpVisitorDPCaching visitor, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping) {
		int size = visitor.bgpIds.size();
        long bliss = create();
        assert bliss != 0;
		_bliss_map     = new TreeMap<V,Integer>();
		_bliss_map_inv = new TreeMap<Integer,V>();
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    int id = (Integer) v;
		    if(id>=size){
		    	id-=size;
		    }
		    if(id>=size){
		    	id-=size;
		    }
		    if(selectedVertices.get(id)){
			    Vertex vertex = e.getValue();
			    int bliss_vertex = _add_vertex(bliss, vertex.color);
			    _bliss_map.put(v, bliss_vertex);
			    _bliss_map_inv.put(bliss_vertex,v);
		    }
		}
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    Vertex vertex = e.getValue();
		    for(Vertex vertex2 : vertex.edges) {
		    	if(_bliss_map.containsKey(vertex.id) && _bliss_map.containsKey(vertex2.id)){
		    		   _add_edge(bliss,
		    				_bliss_map.get(vertex.id),
		    				_bliss_map.get(vertex2.id));
		    	}
		    }
		}
		_reporter = null;
		_reporter_param = null;
	        int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);
		TreeMap<Integer,Integer> labeling = new TreeMap<Integer,Integer>();
		String label="";
		
		for(Map.Entry<V,Integer> e : _bliss_map.entrySet()){
		    labeling.put(cf[e.getValue()],(Integer)e.getKey());
		}
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		HashMap<Integer, Long> retSelective = new HashMap<Integer, Long>();
		int numVars = visitor.numVars;
		for(Integer i:labeling.values()){
			if(selectedVertices.get(i)){
				ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
				Triple tr = visitor.tripleIds.get(i);
				if(btr.getS()>0 && btr.getS()<visitor.selectivityOffset){
					label+=btr.getS()+"_";
				}
				else{
					Integer varId;
					if(btr.getS()==0){
						varId = varlabels.get((Var)tr.getSubject());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getSubject(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getS());
						//varlabels.put("$"+varlabels.size()+1, varId);
					}
					label+="$"+varId+"_";
				}
				if(btr.getP()>0 && btr.getP()<visitor.selectivityOffset){
					label+=btr.getP()+"_";
				}
				else{
					Integer varId;
					if(btr.getP()==0){
						varId = varlabels.get((Var)tr.getPredicate());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getPredicate(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getP());
					}
					label+="$"+varId+"_";
				}
				if(btr.getO()>0 && btr.getO()<visitor.selectivityOffset)
					label+=btr.getO()+"&";
				else{
					Integer varId;
					if(btr.getO()==0){
						varId = varlabels.get((Var)tr.getObject());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getObject(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getO());
					}
					label+="$"+varId+"&";
				}
				
				//label+=btr.getS()+"_"+btr.getP()+"_"+btr.getO()+"&";
			}
		}

		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			//System.out.println("VarLabels: "+e.getKey()+" "+e.getValue());
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		int k=0;
		for(Map.Entry<Integer,Long> e : retSelective.entrySet()){
			//System.out.println("selectiveBindings: "+e.getKey()+" "+e.getValue());
			int t = numVars+k;
			//System.out.println("selective VarMapping: "+e.getKey()+" "+t);
			canonicalVarMapping.put(e.getKey(), t);
			selectiveBindings.put(t, e.getValue());
			k++;
		}
		_bliss_map = null;
		_bliss_map_inv = null;
		_reporter = null;
		_reporter_param = null;
		//System.out.println("Label:"+label);
		return label;
	}

	public String canonical_labeling2(BitSet selectiveIds, BitSet selectedEdges,
			OptimizeOpVisitorDPCaching visitor, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping) {
		int size = visitor.bgpIds.size();
        long bliss = create();
        assert bliss != 0;
		_bliss_map     = new TreeMap<V,Integer>();
		_bliss_map_inv = new TreeMap<Integer,V>();
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    int id = (Integer) v;
		    if(id>=size){
		    	id-=size;
		    }
		    if(id>=size){
		    	id-=size;
		    }
		    if(selectedEdges.get(id)){
			    Vertex vertex = e.getValue();
			    int bliss_vertex = _add_vertex(bliss, vertex.color);
			    _bliss_map.put(v, bliss_vertex);
			    _bliss_map_inv.put(bliss_vertex,v);
		    }
		}
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    Vertex vertex = e.getValue();
		    for(Vertex vertex2 : vertex.edges) {
		    	if(_bliss_map.containsKey(vertex.id) && _bliss_map.containsKey(vertex2.id)){
		    		   _add_edge(bliss,
		    				_bliss_map.get(vertex.id),
		    				_bliss_map.get(vertex2.id));
		    	}
		    }
		}
		_reporter = null;
		_reporter_param = null;
	        int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);
		TreeMap<Integer,Integer> labeling = new TreeMap<Integer,Integer>();
		String label="";
		
		for(Map.Entry<V,Integer> e : _bliss_map.entrySet()){
		    labeling.put(cf[e.getValue()],(Integer)e.getKey());
		}
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		HashMap<Integer, Long> retSelective = new HashMap<Integer, Long>();
		int numVars = visitor.numVars;
		for(Integer i:labeling.values()){
			if(selectedEdges.get(i)){
				ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
	    		int ii = i*3;
				Triple tr = visitor.tripleIds.get(i);
				if(btr.getS()>0 && !selectiveIds.get(ii)){
					label+=btr.getS()+"_";
				}
				else{
					Integer varId;
					if(btr.getS()==0){
						varId = varlabels.get((Var)tr.getSubject());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getSubject(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getS());
						//varlabels.put("$"+varlabels.size()+1, varId);
					}
					label+="$"+varId+"_";
				}
				if(btr.getP()>0 && !selectiveIds.get(ii+1)){
					label+=btr.getP()+"_";
				}
				else{
					Integer varId;
					if(btr.getP()==0){
						varId = varlabels.get((Var)tr.getPredicate());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getPredicate(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getP());
					}
					label+="$"+varId+"_";
				}
				if(btr.getO()>0 && !selectiveIds.get(ii+2))
					label+=btr.getO()+"&";
				else{
					Integer varId;
					if(btr.getO()==0){
						varId = varlabels.get((Var)tr.getObject());
						if(varId==null){
							varId = varlabels.size()+retSelective.size()+1;
							varlabels.put((Var)tr.getObject(), varId);
						}
					}
					else{//removed selective id
						varId = varlabels.size()+retSelective.size()+1;
						retSelective.put(varId, btr.getO());
					}
					label+="$"+varId+"&";
				}
				
				//label+=btr.getS()+"_"+btr.getP()+"_"+btr.getO()+"&";
			}
		}

		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			//System.out.println("VarLabels: "+e.getKey()+" "+e.getValue());
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		int k=0;
		for(Map.Entry<Integer,Long> e : retSelective.entrySet()){
			//System.out.println("selectiveBindings: "+e.getKey()+" "+e.getValue());
			int t = numVars+k;
			//System.out.println("selective VarMapping: "+e.getKey()+" "+t);
			canonicalVarMapping.put(e.getKey(), t);
			selectiveBindings.put(t, e.getValue());
			k++;
		}
		_bliss_map = null;
		_bliss_map_inv = null;
		_reporter = null;
		_reporter_param = null;
		//System.out.println("Label:"+label);
		return label;
	}

	public String canonical_labelingStars(BitSet selectiveIds, BitSet selectedEdges,
			OptimizeOpVisitorDPCaching visitor, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping) {
		
		
		int size = visitor.bgpIds.size();
        long bliss = create();
        assert bliss != 0;
		_bliss_map     = new TreeMap<V,Integer>();
		_bliss_map_inv = new TreeMap<Integer,V>();
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    int id = (Integer) v;
		    if(id>=size){
		    	id-=size;
		    }
		    if(id>=size){
		    	id-=size;
		    }
		    if(selectedEdges.get(id)){
			    Vertex vertex = e.getValue();
			    int bliss_vertex = _add_vertex(bliss, vertex.color);
			    _bliss_map.put(v, bliss_vertex);
			    _bliss_map_inv.put(bliss_vertex,v);
		    }
		}
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    Vertex vertex = e.getValue();
		    for(Vertex vertex2 : vertex.edges) {
		    	if(_bliss_map.containsKey(vertex.id) && _bliss_map.containsKey(vertex2.id)){
		    		   _add_edge(bliss,
		    				_bliss_map.get(vertex.id),
		    				_bliss_map.get(vertex2.id));
		    	}
		    }
		}
		_reporter = null;
		_reporter_param = null;
	        int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);
		TreeMap<Integer,Integer> labeling = new TreeMap<Integer,Integer>();
		String label="";
		
		for(Map.Entry<V,Integer> e : _bliss_map.entrySet()){
		    labeling.put(cf[e.getValue()],(Integer)e.getKey());
		}
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		HashMap<Integer, Long> retSelective = new HashMap<Integer, Long>();
		int numVars = visitor.numVars;
		for(Integer i:labeling.values()){
			if(selectedEdges.get(i)){
				ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
				label += appendLabel(btr, varlabels, retSelective, i, visitor, selectiveIds);
				//label+=btr.getS()+"_"+btr.getP()+"_"+btr.getO()+"&";
			}
		}

		HashMap<Var, Integer> varlabels2 = new HashMap<Var, Integer>();
		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			varlabels2.put(e.getKey(), e.getValue());
		}
		
		for(Map.Entry<Var,Integer> e : varlabels2.entrySet()){
			//System.out.println("VarLabels: "+e.getKey()+" "+e.getValue());
			Pair<Integer, BitSet> star = visitor.starsVar.get(visitor.varRevIds.get(e.getKey()));
			if(star!=null){
				BitSet b = star.getSecond();
				//System.out.println("Found star var: "+b);
				TreeMap<ByteTriple,Integer> starEdges = new TreeMap<ByteTriple,Integer>();
				for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i+1)) {
					if(!selectedEdges.get(i)){
						starEdges.put(visitor.bgpIds.get(i).byteTriples.get(0),i);
					}
				}
				label+="{";
				for(Entry<ByteTriple,Integer> btr : starEdges.entrySet()){
					label+=appendLabel(btr.getKey(), varlabels, retSelective, btr.getValue(), visitor, selectiveIds);
				}
				label+="}";
			}
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		int k=0;
		for(Map.Entry<Integer,Long> e : retSelective.entrySet()){
			//System.out.println("selectiveBindings: "+e.getKey()+" "+e.getValue());
			int t = numVars+k;
			//System.out.println("selective VarMapping: "+e.getKey()+" "+t);
			canonicalVarMapping.put(e.getKey(), t);
			selectiveBindings.put(t, e.getValue());
			k++;
		}
		_bliss_map = null;
		_bliss_map_inv = null;
		_reporter = null;
		_reporter_param = null;
		//System.out.println("Label:"+label);
		return label;
	}

	public String canonical_labelingStars2(BitSet selectiveIds, BitSet selectedEdges, BitSet skeletonEdges,
			OptimizeOpVisitorDPCaching visitor, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping,
			List<Pair<Integer, TreeSet<String>>> starLabels) {
		
		
		int size = visitor.bgpIds.size();
        long bliss = create();
        assert bliss != 0;
		_bliss_map     = new TreeMap<V,Integer>();
		_bliss_map_inv = new TreeMap<Integer,V>();
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    int id = (Integer) v;
		    if(id>=size){
		    	id-=size;
		    }
		    if(id>=size){
		    	id-=size;
		    }
		    if(skeletonEdges.get(id)){
			    Vertex vertex = e.getValue();
			    int bliss_vertex = _add_vertex(bliss, vertex.color);
			    _bliss_map.put(v, bliss_vertex);
			    _bliss_map_inv.put(bliss_vertex,v);
		    }
		}
		for(Map.Entry<V,Vertex> e : vertices.entrySet()) {
		    V v = e.getKey();
		    Vertex vertex = e.getValue();
		    for(Vertex vertex2 : vertex.edges) {
		    	if(_bliss_map.containsKey(vertex.id) && _bliss_map.containsKey(vertex2.id)){
		    		   _add_edge(bliss,
		    				_bliss_map.get(vertex.id),
		    				_bliss_map.get(vertex2.id));
		    	}
		    }
		}
		_reporter = null;
		_reporter_param = null;
	        int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);
		TreeMap<Integer,Integer> labeling = new TreeMap<Integer,Integer>();
		String label="";
		
		for(Map.Entry<V,Integer> e : _bliss_map.entrySet()){
		    labeling.put(cf[e.getValue()],(Integer)e.getKey());
		}
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		HashMap<Integer, Long> retSelective = new HashMap<Integer, Long>();
		int numVars = visitor.numVars;
		for(Integer i:labeling.values()){
			if(skeletonEdges.get(i)){
				ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
				label += appendLabel(btr, varlabels, retSelective, i, visitor, selectiveIds);
				//label+=btr.getS()+"_"+btr.getP()+"_"+btr.getO()+"&";
			}
		}

		TreeMap<Integer, Var> varlabels2 = new TreeMap<Integer, Var>();
		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			varlabels2.put(e.getValue(), e.getKey());
		}
		
		for(Map.Entry<Integer,Var> e : varlabels2.entrySet()){
			//System.out.println("VarLabels: "+e.getKey()+" "+e.getValue());
			Pair<Integer, BitSet> star = visitor.starsVar.get(visitor.varRevIds.get(e.getValue()));
			if(star!=null){
				TreeMap<String,List<Integer>> starEdges = new TreeMap<String,List<Integer>>();
				if(selectedEdges.get(star.getFirst())){//add star triples not skeleton
					BitSet b = new BitSet();
					b.or(star.getSecond());
					b.andNot(visitor.skeletonTriples);
					for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i+1)) {
						String l = getBTRLabel(i, visitor, selectiveIds);
						List<Integer> v = starEdges.get(l);
						if(v!=null)
							v.add(i);
						else{
							v = new ArrayList<Integer>();
							v.add(i);
							starEdges.put(l, v);
						}
					}
				}
				BitSet b = new BitSet();
				b.or(star.getSecond());
				b.and(visitor.skeletonTriples);
				b.and(selectedEdges);
				b.andNot(skeletonEdges);
				for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i+1)) {
					String l = getBTRLabel(i, visitor, selectiveIds);
					List<Integer> v = starEdges.get(l);
					if(v!=null)
						v.add(i);
					else{
						v = new ArrayList<Integer>();
						v.add(i);
						starEdges.put(l, v);
					}
				}
				
				
				//System.out.println("Found star var: "+b);
				if(!starEdges.isEmpty()){
					//starLabels.add(new Pair<Integer, TreeSet<String>>(e.getKey(), starEdges));
					//label+="{";
					TreeSet<String> stl = new TreeSet<String>();
					for(Entry<String,List<Integer>> btr : starEdges.entrySet()){
						for(Integer i : btr.getValue()){
							String l=appendLabel2(visitor.bgpIds.get(i).byteTriples.get(0), varlabels, retSelective, i, visitor, selectiveIds);
							stl.add(l);
						}
					}
					starLabels.add(new Pair<Integer, TreeSet<String>>(e.getKey(), stl));
					//label+="}";*/
				}
			}
			//canonicalVarMapping.put(e.getKey(), visitor.varRevIds.get(e.getValue()));
		}

		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		
		int k=0;
		for(Map.Entry<Integer,Long> e : retSelective.entrySet()){
			//System.out.println("selectiveBindings: "+e.getKey()+" "+e.getValue());
			int t = numVars+k;
			//System.out.println("selective VarMapping: "+e.getKey()+" "+t);
			canonicalVarMapping.put(e.getKey(), t);
			selectiveBindings.put(t, e.getValue());
			k++;
		}
		_bliss_map = null;
		_bliss_map_inv = null;
		_reporter = null;
		_reporter_param = null;
		//System.out.println("Label:"+label);
		return label;
	}


	public TreeSet<String> canonical_labelingStarsNoSkeleton(BitSet selectiveIds, BitSet selectedEdges,
			OptimizeOpVisitorDPCaching visitor, HashMap<Integer, Long> selectiveBindings, TreeMap<Integer, Integer> canonicalVarMapping) {
		TreeSet<String> ret = new TreeSet<String>();

		HashMap<Integer, Long> retSelective = new HashMap<Integer, Long>();
		HashMap<Var, Integer> varlabels =new HashMap<Var, Integer>();
		Integer starId = 0; 
		for (int i = selectedEdges.nextSetBit(0); i >= 0; i = selectedEdges.nextSetBit(i+1)) {
			if(visitor.starsIDs.containsKey(i)){
				starId =i;
				varlabels.put(visitor.varIds.get(visitor.starsIDs.get(i).getFirst()), 1);//starVar == $1
				break;
			}
		}
		
		TreeMap<String,List<Integer>> starEdges = new TreeMap<String,List<Integer>>();
		BitSet starTriples = new BitSet();
		starTriples.or(visitor.starsIDs.get(starId).getSecond());
		starTriples.andNot(visitor.skeletonTriples);
		starTriples.or(selectedEdges);
		starTriples.clear(starId);
		System.out.println("StarTriples: "+starTriples);
		for (int i = starTriples.nextSetBit(0); i >= 0; i = starTriples.nextSetBit(i+1)) {
			String l = getBTRLabel(i, visitor, selectiveIds);
			List<Integer> v = starEdges.get(l);
			if(v!=null)
				v.add(i);
			else{
				v = new ArrayList<Integer>();
				v.add(i);
				starEdges.put(l, v);
			}
		}
		if(!starEdges.isEmpty()){
			for(Entry<String,List<Integer>> btr : starEdges.entrySet()){
				for(Integer i : btr.getValue()){
					String l=appendLabel2(visitor.bgpIds.get(i).byteTriples.get(0), varlabels, retSelective, i, visitor, selectiveIds);
					ret.add(l);
				}
			}
		}
		for(Map.Entry<Var,Integer> e : varlabels.entrySet()){
			canonicalVarMapping.put(e.getValue(), visitor.varRevIds.get(e.getKey()));
		}
		int k=0;
		int numVars = visitor.numVars;
		for(Map.Entry<Integer,Long> e : retSelective.entrySet()){
			//System.out.println("selectiveBindings: "+e.getKey()+" "+e.getValue());
			int t = numVars+k;
			//System.out.println("selective VarMapping: "+e.getKey()+" "+t);
			canonicalVarMapping.put(e.getKey(), t);
			selectiveBindings.put(t, e.getValue());
			k++;
		}
		return ret;
	}
	
	
	private String getBTRLabel(int i, OptimizeOpVisitorDPCaching visitor, BitSet selectiveIds) {
		ByteTriple btr = visitor.bgpIds.get(i).byteTriples.get(0);
		String label ="";
		int ii = i*3;
		if(btr.getS()>0 && !selectiveIds.get(ii)){
			label+=btr.getS()+"_";
		}
		else{
			label+="0_";
		}
		if(btr.getP()>0 && !selectiveIds.get(ii+1)){
			label+=btr.getP()+"_";
		}
		else{
			label+="0_";
		}
		if(btr.getO()>0 && !selectiveIds.get(ii+2))
			label+=btr.getO();
		else{
			label+="0";
		}
		return label;
	}

	private String appendLabel2(ByteTriple btr, HashMap<Var, Integer> varlabels,
			HashMap<Integer, Long> retSelective, Integer i, OptimizeOpVisitorDPCaching visitor, BitSet selectiveIds) {
		String label ="";
		int ii = i*3;
		Triple tr = visitor.tripleIds.get(i);
		//System.out.println(i+" "+btr.toString());
		if(btr.getS()>0 && !selectiveIds.get(ii)){
			label+=btr.getS()+"_";
		}
		else{
			Integer varId;
			if(btr.getS()==0){
				varId = varlabels.get((Var)tr.getSubject());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getSubject(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getS());
				//varlabels.put("$"+varlabels.size()+1, varId);
			}
			label+="$"+varId+"_";
		}
		if(btr.getP()>0 && !selectiveIds.get(ii+1)){
			label+=btr.getP()+"_";
		}
		else{
			Integer varId;
			if(btr.getP()==0){
				varId = varlabels.get((Var)tr.getPredicate());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getPredicate(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getP());
			}
			label+="$"+varId+"_";
		}
		if(btr.getO()>0 && !selectiveIds.get(ii+2))
			label+=btr.getO();
		else{
			Integer varId;
			if(btr.getO()==0){
				varId = varlabels.get((Var)tr.getObject());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getObject(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getO());
			}
			label+="$"+varId;
		}
		return label;
	}

	private String appendLabel(ByteTriple btr, HashMap<Var, Integer> varlabels,
			HashMap<Integer, Long> retSelective, Integer i, OptimizeOpVisitorDPCaching visitor, BitSet selectiveIds) {
		String label ="";
		int ii = i*3;
		Triple tr = visitor.tripleIds.get(i);
		//System.out.println(i+" "+btr.toString());
		if(btr.getS()>0 && !selectiveIds.get(ii)){
			label+=btr.getS()+"_";
		}
		else{
			Integer varId;
			if(btr.getS()==0){
				varId = varlabels.get((Var)tr.getSubject());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getSubject(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getS());
				//varlabels.put("$"+varlabels.size()+1, varId);
			}
			label+="$"+varId+"_";
		}
		if(btr.getP()>0 && !selectiveIds.get(ii+1)){
			label+=btr.getP()+"_";
		}
		else{
			Integer varId;
			if(btr.getP()==0){
				varId = varlabels.get((Var)tr.getPredicate());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getPredicate(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getP());
			}
			label+="$"+varId+"_";
		}
		if(btr.getO()>0 && !selectiveIds.get(ii+2))
			label+=btr.getO()+"&";
		else{
			Integer varId;
			if(btr.getO()==0){
				varId = varlabels.get((Var)tr.getObject());
				if(varId==null){
					varId = varlabels.size()+retSelective.size()+1;
					varlabels.put((Var)tr.getObject(), varId);
				}
			}
			else{//removed selective id
				varId = varlabels.size()+retSelective.size()+1;
				retSelective.put(varId, btr.getO());
			}
			label+="$"+varId+"&";
		}
		return label;
	}


}
