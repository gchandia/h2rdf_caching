/*******************************************************************************
 * Copyright (c) 2012 Nikos Papailiou.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Nikos Papailiou - initial API and implementation
 ******************************************************************************/
package gr.ntua.h2rdf.examples;

import fi.tkk.ics.jbliss.Digraph;
import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.H2RDFQueryResult;
import gr.ntua.h2rdf.client.ResultSet;
import gr.ntua.h2rdf.client.Store;
import gr.ntua.h2rdf.dpplanner.CachingExecutor;
import gr.ntua.h2rdf.indexScans.BGP;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCachingOld;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.PatternVars;

import java.io.IOException;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

public class CanonicalLabelExample {

	private static HashMap<Integer, Var> varIds;
	private static HashMap<Var, Integer> varRevIds;
	private static HashMap<Integer, Triple> tripleIds;
	private static HashMap<Integer, BGP> bgpIds;
	private static HashMap<Triple, Integer> tripleRevIds;
	private static TreeMap<Integer, BitSet> varGraph;
	private static TreeMap<Integer, TreeMap<Integer, BitSet>> edgeGraph;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String address = "master";
		String table = args[0];
		String user = "root";

		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
				"PREFIX yago: <http://www.w3.org/2000/01/rdf-schema#>"+
				"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>";
		String NL = System.getProperty("line.separator") ;
		
		String q1 = prolog + NL +
				"SELECT  * " +
		        "WHERE   { ?x ub:advisor ?z ." +
		        "?x ub:takesCourse ?y ." +
		        "?z ub:teacherOf ?y } ";
		
   
        String q2 = prolog + NL +
		"SELECT  ?x ?y ?z " +
		"WHERE   { " +
		"?x1 rdf:type ub:GraduateStudent ." +
		"?y rdf:type ub:University ." +
		"?z rdf:type ub:Department ." +
		"?x1 ub:memberOf ?z ." +
		"?z ub:subOrganizationOf ?y ." +
		"?x1 ub:undergraduateDegreeFrom ?y " +
		"}";
        
        String q3 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Publication ." +
		"?x ub:publicationAuthor <http://www.Department1.University0.edu/AssistantProfessor5> }";
        
        String q4 = prolog + NL +
		"SELECT  ?x ?n ?em ?t" +
		"WHERE   { " +
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t ." +
		"?x ub:worksFor <http://www.Department0.University0.edu> ." +
		"?x rdf:type ub:FullProfessor ."+
		"}";
        
        String q5 = prolog + NL +
		"SELECT  ?x  " +
		"WHERE   {" +
		"?x rdf:type ub:UndergraduateStudent ." +
		"?x ub:memberOf <http://www.Department1.University0.edu> }";
        
        String q6 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Student  }";
        
        String q7 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { " +
		"?y rdf:type ub:Course ." +
		"<http://www.Department0.University0.edu/FullProfessor0> ub:teacherOf ?y ." +
		"?x ub:takesCourse ?y " +
		"}";
        
        String q8 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q9 = prolog + NL +
        		"SELECT  ?x ?z ?y " +
        		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
        		"?z rdf:type ub:FullProfessor ." +
        		"?y rdf:type ub:Course ."+
        		"?x ub:advisor ?z ." +
        		"?x ub:takesCourse ?y ." +
        		"?z ub:teacherOf ?y }";
        
        String q10 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        String q11 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q12 = prolog + NL +
		"SELECT  ?x ?y " +
		"WHERE   { ?x rdf:type ub:Professor ." +
		"?y rdf:type ub:Department ." +
		"?x ub:worksFor ?y ." +
		"?y ub:subOrganizationOf <http://www.University0.edu>}";
        
        String q13 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        String q14 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String qs = prolog + NL +
        		"select * where {" +
        		"?x <http://www.University1.edu> ?x1 . " +
        		"?x <http://www.University2.edu> ?x2 . " +
        		"?x <http://www.University3.edu> ?x3 . " +
        		"?x <http://www.University4.edu> ?y . " +
        		"?y <http://www.University2.edu> ?z . " +
        		"?z <http://www.University1.edu> ?x4 . " +
        		"?z <http://www.University2.edu> ?x5 . " +
        		"?z <http://www.University3.edu> ?x6 }";

        String qs1 = prolog + NL +
        		"select * where {" +
        		"?x <http://www.University1.edu> ?x1 . " +
        		"?x <http://www.University2.edu> ?x2 . " +
        		"?x <http://www.University3.edu> ?x3 . " +
        		"?x <http://www.University1.edu> ?x4 . " +
        		"?x <http://www.University2.edu> ?x5 . " +
        		"?x <http://www.University3.edu> ?x6 . " +
        		"?x <http://www.University1.edu> ?x7 . " +
        		"?x7 <http://www.University2.edu> ?x8 . " +
        		"?x6 <http://www.University3.edu> ?x9 . " +
        		"?x5 <http://www.University1.edu> ?x10 . " +
        		"?x4 <http://www.University2.edu> ?x11 . " +
        		"?x3 <http://www.University3.edu> ?x12 . " +
        		"?x2 <http://www.University1.edu> ?x13 . " +
        		"?x1 <http://www.University2.edu> ?x14 }";

        String qs2 = prolog + NL +
        		"select * where {" +
        		"?prof rdf:type ub:FullProfessor . " +
        		"?prof ub:name ?n ." +
        		"?prof ub:emailAddress ?em ." +
        		"?prof ub:worksFor ?u ." +
        		"?prof ub:memberOf ?p ." +
        		"?prof ub:advisor ?st ." +
        		"?st ub:memberOf ?p ." +
        		"?st rdf:type ub:GraduateStudent . " +
        		"?st ub:name ?n1 ." +
        		"?st ub:emailAddress ?em1 ." +
        		"?st ub:worksFor ?u1 }";
        
        String qpath = prolog + NL +
        		"select * where {" +
        		"?r1 ub:memberOf ?r2 ."+
        		"?r2 ub:memberOf ?r3 ."+
        		"?r3 ub:memberOf ?r4 ."+
        		"?r4 ub:memberOf ?r5 ."+
        		"?r5 ub:memberOf ?r6 ."+
        		"?r6 ub:memberOf ?r7 ."+
        		"?r7 ub:memberOf ?r8 ."+
        		"?r8 ub:memberOf ?r9 ."+
        		"?r9 ub:memberOf ?r10 ."+
        		"?r10 ub:memberOf ?r11 ."+
        		"?r11 ub:memberOf ?r12 ."+
        		"?r12 ub:memberOf ?r13 ."+
        		"?r13 ub:memberOf ?r14 ."+
        		"?r14 ub:memberOf ?r15 ."+
        		"?r15 ub:memberOf ?r16 ."+
        		"?r16 ub:memberOf ?r17 ."+
        		"?r17 ub:memberOf ?r18 ."+
        		"?r18 ub:memberOf ?r19 ."+
        		"?r19 ub:memberOf ?r20 ."+
        		"?r20 ub:memberOf ?r21 ."+
        		"?r21 ub:memberOf ?r22 ."+
        		"?r22 ub:memberOf ?r23 ."+
        		"?r23 ub:memberOf ?r24 ."+
        		"?r24 ub:memberOf ?r25 ."+
        		"?r25 ub:memberOf ?r26 ."+
        		"?r26 ub:memberOf ?r27 ."+
        		"?r27 ub:memberOf ?r28 ."+
        		"?r28 ub:memberOf ?r29 ."+
        		"?r29 ub:memberOf ?r30 ."+
        		"?r30 ub:memberOf ?r31 ."+
        		"?r31 ub:memberOf ?r32 ."+
        		"?r32 ub:memberOf ?r33 ."+
        		"?r33 ub:memberOf ?r34 ."+
        		"?r34 ub:memberOf ?r35 ."+
        		"?r35 ub:memberOf ?r36 ."+
        		"?r36 ub:memberOf ?r37 ."+
        		/*"?r37 ub:memberOf ?r38 ."+
        		"?r38 ub:memberOf ?r39 ."+
        		"?r39 ub:memberOf ?r40 ."+
        		"?r40 ub:memberOf ?r41 ."+*/
        		"}";
        
        String qcycle = prolog + NL +
        		"select * where {" +
        		"?r1 ub:memberOf ?r2 ."+
        		"?r2 ub:memberOf ?r3 ."+
        		"?r3 ub:memberOf ?r4 ."+
        		"?r4 ub:memberOf ?r5 ."+
        		"?r5 ub:memberOf ?r6 ."+
        		"?r6 ub:memberOf ?r7 ."+
        		"?r7 ub:memberOf ?r8 ."+
        		"?r8 ub:memberOf ?r9 ."+
        		"?r9 ub:memberOf ?r10 ."+
        		"?r10 ub:memberOf ?r11 ."+
        		"?r11 ub:memberOf ?r12 ."+
        		"?r12 ub:memberOf ?r13 ."+
        		"?r13 ub:memberOf ?r14 ."+
        		"?r14 ub:memberOf ?r15 ."+
        		"?r15 ub:memberOf ?r16 ."+
        		"?r16 ub:memberOf ?r17 ."+
        		"?r17 ub:memberOf ?r18 ."+
        		"?r18 ub:memberOf ?r19 ."+
        		"?r19 ub:memberOf ?r20 ."+
        		"?r20 ub:memberOf ?r21 ."+
        		"?r21 ub:memberOf ?r22 ."+
        		"?r22 ub:memberOf ?r23 ."+
        		"?r23 ub:memberOf ?r24 ."+
        		"?r24 ub:memberOf ?r25 ."+
        		"?r25 ub:memberOf ?r26 ."+
        		"?r26 ub:memberOf ?r27 ."+
        		"?r27 ub:memberOf ?r28 ."+
        		"?r28 ub:memberOf ?r29 ."+
        		"?r29 ub:memberOf ?r30 ."+
        		"?r30 ub:memberOf ?r31 ."+
        		"?r31 ub:memberOf ?r32 ."+
        		"?r32 ub:memberOf ?r33 ."+
        		"?r33 ub:memberOf ?r34 ."+
        		"?r34 ub:memberOf ?r35 ."+
        		"?r35 ub:memberOf ?r36 ."+
        		"?r36 ub:memberOf ?r1 ."+
        		/*"?r37 ub:memberOf ?r38 ."+
        		"?r38 ub:memberOf ?r39 ."+
        		"?r39 ub:memberOf ?r40 ."+
        		"?r40 ub:memberOf ?r1 ."+*/
        		"}";
        
        String qbanana = prolog + NL +
        		"select * where {" +
        		"?s1 ub:memberOf ?r ."+
        		"?s1 ub:memberOf ?r1 ."+
        		"?s1 ub:memberOf ?r2 ."+
        		"?s1 ub:memberOf ?r3 ."+
        		"?s2 ub:memberOf ?r ."+
        		"?s2 ub:memberOf ?r4 ."+
        		"?s2 ub:memberOf ?r5 ."+
        		"?s2 ub:memberOf ?r6 ."+
        		"?s3 ub:memberOf ?r ."+
        		"?s3 ub:memberOf ?r7 ."+
        		"?s3 ub:memberOf ?r8 ."+
        		"?s3 ub:memberOf ?r9 ."+
        		"?s4 ub:memberOf ?r ."+
        		"?s4 ub:memberOf ?r10 ."+
        		"?s4 ub:memberOf ?r11 ."+
        		"?s4 ub:memberOf ?r12 ."+
        		"?s5 ub:memberOf ?r ."+
        		"?s5 ub:memberOf ?r13 ."+
        		"?s5 ub:memberOf ?r14 ."+
        		"?s5 ub:memberOf ?r15 ."+
        		"?s6 ub:memberOf ?r ."+
        		"?s6 ub:memberOf ?r16 ."+
        		"?s6 ub:memberOf ?r17 ."+
        		"?s6 ub:memberOf ?r18 ."+
        		"?s7 ub:memberOf ?r ."+
        		"?s7 ub:memberOf ?r19 ."+
        		"?s7 ub:memberOf ?r20 ."+
        		"?s7 ub:memberOf ?r21 ."+
        		"?s8 ub:memberOf ?r ."+
        		"?s8 ub:memberOf ?r22 ."+
        		"?s8 ub:memberOf ?r23 ."+
        		"?s8 ub:memberOf ?r24 ."+
        		"?s9 ub:memberOf ?r ."+
        		"?s9 ub:memberOf ?r25 ."+
        		"?s9 ub:memberOf ?r26 ."+
        		"?s9 ub:memberOf ?r27 ."+
        		"?s10 ub:memberOf ?r ."+
        		"?s10 ub:memberOf ?r28 ."+
        		"?s10 ub:memberOf ?r29 ."+
        		"?s10 ub:memberOf ?r30 ."+
        		
        		"?s11 ub:memberOf ?r ."+
        		"?s11 ub:memberOf ?r31 ."+
        		"?s11 ub:memberOf ?r32 ."+
        		"?s11 ub:memberOf ?r33 ."+
        		"?s12 ub:memberOf ?r ."+
        		"?s12 ub:memberOf ?r34 ."+
        		"?s12 ub:memberOf ?r35 ."+
        		"?s12 ub:memberOf ?r36 ."+
        		"?s13 ub:memberOf ?r ."+
        		"?s13 ub:memberOf ?r37 ."+
        		"?s13 ub:memberOf ?r38 ."+
        		"?s13 ub:memberOf ?r39 ."+
        		"?s14 ub:memberOf ?r ."+
        		"?s14 ub:memberOf ?r40 ."+
        		"?s14 ub:memberOf ?r41 ."+
        		"?s14 ub:memberOf ?r42 ."+
        		"?s15 ub:memberOf ?r ."+
        		"?s15 ub:memberOf ?r43 ."+
        		"?s15 ub:memberOf ?r44 ."+
        		"?s15 ub:memberOf ?r45 ."+
        		"?s16 ub:memberOf ?r ."+
        		"?s16 ub:memberOf ?r46 ."+
        		"?s16 ub:memberOf ?r47 ."+
        		"?s16 ub:memberOf ?r48 ."+
        		"?s17 ub:memberOf ?r ."+
        		"?s17 ub:memberOf ?r49 ."+
        		"?s17 ub:memberOf ?r50 ."+
        		"?s17 ub:memberOf ?r51 ."+
        		"?s18 ub:memberOf ?r ."+
        		"?s18 ub:memberOf ?r52 ."+
        		"?s18 ub:memberOf ?r53 ."+
        		"?s18 ub:memberOf ?r54 ."+
        		"?s19 ub:memberOf ?r ."+
        		"?s19 ub:memberOf ?r55 ."+
        		"?s19 ub:memberOf ?r56 ."+
        		"?s19 ub:memberOf ?r57 ."+
        		"}";
        
        String qspider = prolog + NL +
        		"select * where {" +
        		"?s <http://www.University1.edu> ?r1 ."+
        		"?r1 <http://www.University2.edu> ?r2 ."+
        		"?s <http://www.University3.edu> ?r3 ."+
        		"?r3 <http://www.University4.edu> ?r4 ."+
        		"?s <http://www.University5.edu> ?r5 ."+
        		"?r5 <http://www.University6.edu> ?r6 ."+
        		"?s <http://www.University7.edu> ?r7 ."+
        		"?r7 <http://www.University8.edu> ?r8 ."+
        		"?s <http://www.University9.edu> ?r9 ."+
        		"?r9 <http://www.University10.edu> ?r10 ."+
        		"?s <http://www.University11.edu> ?r11 ."+
        		"?r11 <http://www.University12.edu> ?r12 ."+
        		"?s <http://www.University13.edu> ?r13 ."+
        		"?r13 <http://www.University14.edu> ?r14 ."+
        		"?s <http://www.University15.edu> ?r15 ."+
        		"?r15 <http://www.University16.edu> ?r16 ."+
        		"?s <http://www.University17.edu> ?r17 ."+
        		"?r17 <http://www.University18.edu> ?r18 ."+
        		"?s <http://www.University19.edu> ?r19 ."+
        		"?r19 <http://www.University20.edu> ?r20 ."+
        		"?s <http://www.University21.edu> ?r21 ."+
        		"?r21 <http://www.University22.edu> ?r22 ."+
        		"?s <http://www.University23.edu> ?r23 ."+
        		"?r23 <http://www.University24.edu> ?r24 ."+
        		"?s <http://www.University25.edu> ?r25 ."+
        		"?r25 <http://www.University26.edu> ?r26 ."+
        		"?s <http://www.University27.edu> ?r27 ."+
        		"?r27 <http://www.University28.edu> ?r28 ."+
        		"?s <http://www.University29.edu> ?r29 ."+
        		"?r29 <http://www.University30.edu> ?r30 ."+
        		"?s <http://www.University31.edu> ?r31 ."+
        		"?r31 <http://www.University32.edu> ?r32 ."+
        		"?s <http://www.University33.edu> ?r33 ."+
        		"?r33 <http://www.University34.edu> ?r34 ."+
        		"?s <http://www.University35.edu> ?r35 ."+
        		"?r35 <http://www.University36.edu> ?r36 ."+
        		"?s <http://www.University37.edu> ?r37 ."+
        		"?r37 <http://www.University38.edu> ?r38 ."+
        		"?s <http://www.University39.edu> ?r39 ."+
        		"?r39 <http://www.University40.edu> ?r40 ."+
        		"}";
        
        String qstar = prolog + NL +
        		"select * where {" +
        		"?s ub:memberOf ?r1 ."+
        		"?s ub:memberOf ?r2 ."+
        		"?s ub:memberOf ?r3 ."+
        		"?s ub:memberOf ?r4 ."+
        		"?s ub:memberOf ?r5 ."+
        		"?s ub:memberOf ?r6 ."+
        		"?s ub:memberOf ?r7 ."+
        		"?s ub:memberOf ?r8 ."+
        		"?s ub:memberOf ?r9 ."+
        		"?s ub:memberOf ?r10 ."+
        		"?s ub:memberOf ?r11 ."+
        		"?s ub:memberOf ?r12 ."+
        		"?s ub:memberOf ?r13 ."+
        		"?s ub:memberOf ?r14 ."+
        		"?s ub:memberOf ?r15 ."+
        		"?s ub:memberOf ?r16 ."+
        		"?s ub:memberOf ?r17 ."+
        		"?s ub:memberOf ?r18 ."+
        		"?s ub:memberOf ?r19 ."+
        		"?s ub:memberOf ?r20 ."+
        		"?s ub:memberOf ?r21 ."+
        		"?s ub:memberOf ?r22 ."+
        		"?s ub:memberOf ?r23 ."+
        		"?s ub:memberOf ?r24 ."+
        		"?s ub:memberOf ?r25 ."+
        		"?s ub:memberOf ?r26 ."+
        		"?s ub:memberOf ?r27 ."+
        		"?s ub:memberOf ?r28 ."+
        		"?s ub:memberOf ?r29 ."+
        		"?s ub:memberOf ?r30 ."+
        		"?s ub:memberOf ?r31 ."+
        		"?s ub:memberOf ?r32 ."+
        		"?s ub:memberOf ?r33 ."+
        		"?s ub:memberOf ?r34 ."+
        		"?s ub:memberOf ?r35 ."+
        		"?s ub:memberOf ?r36 ."+
        		/*"?s ub:memberOf ?r37 ."+
        		"?s ub:memberOf ?r38 ."+
        		"?s ub:memberOf ?r39 ."+
        		"?s ub:memberOf ?r40 ."+*/
        		"}";

        String q="";
        switch (Integer.parseInt(args[2])) {
		case 0:
			q=qpath;
			break;
		case 1:
			q=qcycle;
			break;
		case 2:
			q=qbanana;
			break;
		case 3:
			q=qspider;
			break;
		case 4:
			q=qstar;
			break;
		default:
			break;
		}
        Query query = QueryFactory.create(q) ;
        

		Op opQuery = Algebra.compile(query);
		

		Configuration hconf = HBaseConfiguration.create();
		hconf.set("hbase.rpc.timeout", "3600000");
		hconf.set("zookeeper.session.timeout", "3600000");
		CachingExecutor.connectTable(table, hconf);

        CachingExecutor executor = new CachingExecutor(table,0,Boolean.parseBoolean(args[1]));
        long start = System.currentTimeMillis();
        executor.executeQuery(query,true, true);
        long stop = System.currentTimeMillis();
        //System.out.println(label); 
        double time = (stop-start);
        System.out.println("Exec time: "+time+" ms");
	}
}
