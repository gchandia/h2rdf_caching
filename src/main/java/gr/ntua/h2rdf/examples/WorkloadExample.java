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

import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.bytes.H2RDFNode;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;
import gr.ntua.h2rdf.client.ExecutorOpenRdf;
import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.H2RDFQueryResult;
import gr.ntua.h2rdf.client.ResultSet;
import gr.ntua.h2rdf.client.Store;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.function.library.sqrt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;

public class WorkloadExample {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		String address = "master";
//		String table = "L10";
//		String user = "root";
//		H2RDFConf conf = new H2RDFConf(address, table, user);
//		H2RDFFactory h2fact = new H2RDFFactory();
//		Store store = h2fact.connectStore(conf);
		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> "+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
				"PREFIX y: <http://yago-knowledge.org/resource/>";
		String NL = " ";//System.getProperty("line.separator") ;
		
		
		HashMap<Integer, Double> baselineExecTime = new HashMap<Integer, Double>();
		/*baselineExecTime.put(1, new Long(550));
		baselineExecTime.put(3, new Long(800));
		baselineExecTime.put(4, new Long(1550));
		baselineExecTime.put(5, new Long(3950));
		baselineExecTime.put(7, new Long(1200));
		baselineExecTime.put(1, 412.0);
		baselineExecTime.put(2, 201489.0);
		baselineExecTime.put(3, 501.0);
		baselineExecTime.put(4, 2037.0);
		baselineExecTime.put(5, 3306.0);
		baselineExecTime.put(7, 840.0);
		baselineExecTime.put(9, 284081.0);
		baselineExecTime.put(10, 365616.0);*/

		HashMap<Integer, Long> queryCount = new HashMap<Integer, Long>();
		HashMap<Integer, Long> queryExecTime = new HashMap<Integer, Long>();
		
		
		BitSet b = new BitSet();
		b.set(1);
		b.set(2);
		b.set(3);
		b.set(4);
		b.set(5);
		//b.set(6);
		b.set(7);
		b.set(8);
		b.set(9);
		b.set(10);
		b.set(11);
		b.set(12);
		b.set(13);
		b.set(14);
		
//		b.set(20);
//		b.set(21);
//		b.set(22);
//		b.set(23);
//		b.set(24);
//		b.set(25);
//		b.set(26);

//		b.set(30);
//		b.set(31);
//		b.set(32);
//		b.set(33);
		
//		b.set(15);
//		b.set(16);
//		b.set(3);
//		b.set(4);
//		b.set(5);
//		b.set(7);
//		b.set(2);
//		b.set(9);
//		b.set(10);
		/*b.set(13);
		b.set(14);
		b.set(15);
		b.set(16);*/
		
		Random r = new Random();
		List<String> st = new ArrayList<String>();
		st.add("ub:GraduateStudent");
		st.add("ub:UndergraduateStudent");
		List<String> course = new ArrayList<String>();
		course.add("ub:GraduateCourse");
		course.add("ub:Course");
		List<String> prof = new ArrayList<String>();
		prof.add("ub:FullProfessor");
		prof.add("ub:AssistantProfessor");
		prof.add("ub:AssociateProfessor");
		prof.add("ub:Lecturer");
		List<String> dep = new ArrayList<String>();
		List<String> un = new ArrayList<String>();
		String[] qi = new String[50];
		for (int i = 10000; i < 11000; i++) {
			String u = "http://www.University"+i+".edu";
			un.add(u);

			if(dep.size()<1000){
				for (int j = 0; j < 15; j++) {
					String s = "http://www.Department"+j+".University"+(i/*+30000*/)+".edu";
					dep.add(s);
				}
			}
		}
		
		

		List<String> cities = new ArrayList<String>();
		List<String> names = new ArrayList<String>();
		List<String> descriptors = new ArrayList<String>();
		List<String> journals = new ArrayList<String>();
		List<String> pubmedIds = new ArrayList<String>();

		try {
			BufferedReader br = new BufferedReader(new FileReader("../yagoCities"));
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		    	cities.add(line);
		        line = br.readLine();
		    }
		    br.close();
			br = new BufferedReader(new FileReader("../yagoGivenNames"));
		    sb = new StringBuilder();
		    line = br.readLine();

		    while (line != null) {
		    	names.add(line);
		        line = br.readLine();
		    }
		    br.close();

			br = new BufferedReader(new FileReader("../descriptor"));
		    sb = new StringBuilder();
		    line = br.readLine();

		    while (line != null) {
		    	descriptors.add(line);
		        line = br.readLine();
		    }
		    br.close();
			br = new BufferedReader(new FileReader("../journal"));
		    sb = new StringBuilder();
		    line = br.readLine();

		    while (line != null) {
		    	journals.add(line);
		        line = br.readLine();
		    }
		    br.close();
			br = new BufferedReader(new FileReader("../pubmedIds"));
		    sb = new StringBuilder();
		    line = br.readLine();

		    while (line != null) {
		    	pubmedIds.add(line);
		        line = br.readLine();
		    }
		    br.close();
		    
		    
		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println(cities.get(1));
		System.out.println(names.get(2));

		
		
//		long start = System.currentTimeMillis();
//		try {
//			HTable indexTable = new HTable(conf.getConf(), "L10_Index");
//			for (int i = 0; i < 100000; i++) {
//				if(un.size()<100){
//					String u = "http://www.University"+i+".edu";
//					//System.out.println(u);
//					H2RDFNode n = new H2RDFNode(Node.createURI(u));
//					Get get = new Get(Bytes.toBytes(n.getString()));
//					get.addColumn(Bytes.toBytes("1"), new byte[0]);
//					Result res = indexTable.get(get);
//					if(!res.isEmpty()){
//						//System.out.println("Found: "+u);
//						un.add(u);
//					}
//				}
//				if(dep.size()<1000){
//					for (int j = 0; j < 15; j++) {
//						String s = "http://www.Department"+j+".University"+(i/*+30000*/)+".edu";
//						//System.out.println(s);
//						H2RDFNode n1 = new H2RDFNode(Node.createURI(s));
//						//System.out.println(n1.getString());
//						Get get = new Get(Bytes.toBytes(n1.getString()));
//						get.addColumn(Bytes.toBytes("1"), new byte[0]);
//						Result res1 = indexTable.get(get);
//						if(!res1.isEmpty()){
//							//System.out.println("Found: "+s);
//							dep.add(s);
//						}
//						
//					}
//				}
//				if(un.size()>=100 && dep.size()>=1000)
//					break;
//				//System.out.println(i);
//			}
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		} catch (NotSupportedDatatypeException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//System.out.println(dep.size()+ " "+ un.size());
		Double primitiveCost=0.0;
		Double costSaving=0.0;
		HashMap<Integer,Double> primitiveCostMap = new HashMap<Integer, Double>();
		HashMap<Integer,Double> costSavingMap = new HashMap<Integer, Double>();
		
		LinkedList<Long> geomean = new LinkedList<Long>();
		String subgraph = 
		        "?x ub:memberOf ?z ." +
		        "?z ub:subOrganizationOf ?y ." +
		        "?x ub:undergraduateDegreeFrom ?y " ;
		
		
		
		BufferedWriter output = null;
        File file = new File("lubm.sparql");
        output = new BufferedWriter(new FileWriter(file));
		
		for (int i = 0; i < 10000; i++) {
			String s = dep.get(r.nextInt(dep.size()));
			String u = un.get(r.nextInt(un.size()));
			String stud = st.get(r.nextInt(st.size()));
			String profes = prof.get(r.nextInt(prof.size()));
			String c = course.get(r.nextInt(course.size()));
			

			String city = cities.get(r.nextInt(cities.size()));
			String name = names.get(r.nextInt(names.size()));

			String descr = descriptors.get(r.nextInt(descriptors.size()));
			String journal = journals.get(r.nextInt(journals.size()));
			String pid = pubmedIds.get(r.nextInt(pubmedIds.size()));
			
			
			
			
			qi[1] = prolog + NL +
			"SELECT  * " +
			"WHERE   { ?x ub:takesCourse <"+s+"/GraduateCourse0> ." +
			"?x rdf:type ub:GraduateStudent ."+
			"}";
			
	        qi[2] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { " +
	        "?x rdf:type "+stud+" ." +
	        "?y rdf:type ub:University ." +
	        "?z rdf:type ub:Department ." +
	        "?x ub:memberOf ?z ." +
	        "?z ub:subOrganizationOf ?y ." +
	        "?x ub:undergraduateDegreeFrom ?y " +
	        "}";
			
			qi[3] = prolog + NL +
			"SELECT  * " +
			"WHERE   { ?x ub:publicationAuthor <"+s+"/AssistantProfessor0> ." +
			"?x rdf:type ub:Publication ."+
			"}";
			
	        qi[4] = prolog + NL +
			"SELECT * " +
			"WHERE   { ?x ub:worksFor <"+s+"> ." +
			"?x rdf:type ub:AssistantProfessor ."+
			"?x ub:name ?n ." +
			"?x ub:emailAddress ?em ." +
			"?x ub:telephone ?t " +
			"}";
	        
			qi[5] = prolog + NL +
			"SELECT  * " +
			"WHERE   { ?x ub:memberOf <"+s+"> ." +
			"?x rdf:type ub:UndergraduateStudent ."+
			"}";
	        
	        qi[7] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { " +
	        "?x rdf:type ub:GraduateStudent ." +
	        "?y rdf:type ub:GraduateCourse ." +
	        "<"+s+"/FullProfessor0> ub:teacherOf ?y ." +
	        "?x ub:takesCourse ?y " +
	        "}";
			
	        qi[8] = prolog + NL +
			"SELECT  * WHERE { "  +
			"?x rdf:type ub:UndergraduateStudent . "+
    		"?y rdf:type ub:Department . "+
    		"?x ub:memberOf ?y . " +
    		"?y ub:subOrganizationOf <"+u+"> . " +
    		"?x ub:emailAddress ?em " +
    		"}";
			
	        qi[9] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { ?x rdf:type "+stud+" ." +
	        "?z rdf:type "+profes+" ." +
	        "?y rdf:type "+c+" ."+
	        "?x ub:advisor ?z ." +
	        "?x ub:takesCourse ?y ." +
	        "?z ub:teacherOf ?y }";

	        qi[11] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { ?x rdf:type ub:GraduateStudent ." +
	        "?z rdf:type ub:FullProfessor ." +
	        "?y rdf:type ub:Course ."+
	        "?x ub:advisor ?z ." +
	        "?x ub:takesCourse ?y ." +
	        "?z ub:teacherOf ?y }";
	        qi[10] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { ?p rdf:type ?tp . " +
	        "?p ub:worksFor ?d . " +
	        "?p ub:teacherOf ?c . " +
	        "?s ub:takesCourse ?c } ";
	        
	        qi[12] = prolog + NL +
	        "SELECT  * " +
	        "WHERE   { ?x rdf:type ub:FullProfessor ." +
	        "?y rdf:type ub:Department ." +
	        "?x ub:worksFor ?y ." +
	        "?y ub:subOrganizationOf <"+u+">" +
	        "}";
	        
	        qi[13] = prolog + NL +
	    	        "SELECT  * WHERE { " +
	    	        "?x rdf:type "+stud+" ." +
	    	        "?y rdf:type ub:University ." +
	    	        "?z rdf:type ub:Department ." +
	        		subgraph+
	    	        "}";
	        
	        qi[14] = prolog + NL +
	    	        "SELECT  * WHERE { " +
	    	        "?x ub:name ?n ." +
	    			"?x ub:emailAddress ?em ." +
	    			"?x ub:telephone ?t ." +
	        		subgraph+
	    	        "}";	    
	        
	        qi[15] = prolog + NL +
	    	        "SELECT  * WHERE { " +
	    	        "?z ub:subOrganizationOf <"+u+"> . "+
	        		subgraph+
	    	        "}";        
	        
	        qi[16] = prolog + NL +
	    	        "SELECT  * WHERE { " +
	    	        "?x ub:memberOf <"+s+"> . " +
	        		subgraph+
	    	        "}";    
	        
	        
	        
	        qi[20] = prolog + NL +
	    	        "SELECT * WHERE{ ?p y:hasGivenName ?gn . ?p y:hasFamilyName ?fn . ?p y:wasBornIn ?city1 . ?p y:hasAcademicAdvisor ?a . ?a y:wasBornIn ?city }";
	        qi[21] = prolog + NL +
	    	        "SELECT * WHERE{ ?p y:hasGivenName ?gn . ?p y:hasFamilyName ?fn . ?p y:wasBornIn ?city . ?p y:hasAcademicAdvisor ?a . ?a y:wasBornIn ?city . ?p y:isMarriedTo ?p2 . ?p2 y:wasBornIn ?city1 }";
	        qi[22] = prolog + NL +
	    	        "SELECT * WHERE{ ?a1 y:hasPreferredName ?name1 . ?a2 y:hasPreferredName ?name2 . ?a1 y:actedIn ?movie . ?a2 y:actedIn ?movie }";
	        qi[23] = prolog + NL +
	    	        "SELECT * WHERE{ ?p1 y:hasPreferredName ?name1 . ?p2 y:hasPreferredName ?name2 . ?p1 y:isMarriedTo ?p2 . ?p1 y:wasBornIn ?city . ?p2 y:wasBornIn ?city }";
	        qi[24] = prolog + NL +
	    	        "SELECT * WHERE{ ?p y:hasGivenName " +name+" . ?p y:hasFamilyName ?fn . ?p y:wasBornIn " +city+" . ?p1 y:hasFamilyName ?fn  . ?p1 y:hasGivenName ?gn . ?p1 y:wasBornIn ?city }";
	        qi[25] = prolog + NL +
	    	        "SELECT * WHERE{ ?a1 y:hasGivenName " +name+" . ?a2 y:hasPreferredName ?name2 . ?a1 y:wasBornIn ?c1 . ?a2 y:wasBornIn ?c2 . ?a1 y:actedIn ?movie . ?a2 y:actedIn ?movie }";
	        qi[26] = prolog + NL +
	    	        "SELECT * WHERE{ ?p1 y:hasGivenName " +name+" . ?p2 y:hasPreferredName ?name2 . ?p1 y:isMarriedTo ?p2 . ?p1 y:wasBornIn ?city1 . ?p2 y:wasBornIn ?city }";
	        
	        
	        qi[30] = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?l ."
	    	        + " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t ."
	    	        + " ?s <http://purl.org/dc/terms/identifier> " +pid+" ."
	    	        + " ?s <http://rdfs.org/ns/void#inDataset> ?d ."
	    	        + " ?s <http://bio2rdf.org/pubmed_vocabulary:mesh_heading> ?mh ."
	    	        + " ?mh <http://bio2rdf.org/pubmed_vocabulary:mesh_descriptor_name> ?dn ."
	    	        + " ?mh <http://www.w3.org/2000/01/rdf-schema#label> ?l1 ."
	    	        + " ?mh <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t1 }";
	        qi[31] = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?l ."
	    	        + " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t ."
	    	        + " ?s <http://purl.org/dc/terms/identifier> ?id ."
	    	        + " ?s <http://rdfs.org/ns/void#inDataset> ?d ."
	    	        + " ?s <http://bio2rdf.org/pubmed_vocabulary:mesh_heading> ?mh ."
	    	        + " ?mh <http://bio2rdf.org/pubmed_vocabulary:mesh_descriptor_name> " +descr+" ."
	    	        + " ?mh <http://www.w3.org/2000/01/rdf-schema#label> ?l1 ."
	    	        + " ?mh <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t1 }";

	        qi[32] = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?l ."
	    	        + " ?s <http://bio2rdf.org/pubmed_vocabulary:mesh_heading> ?mh ."
	    	        + " ?mh <http://www.w3.org/2000/01/rdf-schema#label> ?l }";
	        
	        qi[33] = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?l ."
	    	        + " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t ."
	    	        + " ?s <http://purl.org/dc/terms/identifier> ?id ."
	    	        + " ?s <http://rdfs.org/ns/void#inDataset> ?d ."
	    	        + " ?s <http://bio2rdf.org/pubmed_vocabulary:journal> ?j ."
	    	        + " ?j <http://bio2rdf.org/pubmed_vocabulary:journal_title> " +journal+" ."
	    	    	+ " ?j <http://bio2rdf.org/pubmed_vocabulary:journal_volume> ?jv }";
	        
	        String q="";
	        
	        /*for (int t = 0; t < b.cardinality(); t++) {
				int k=0, qid=0;
				for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
					if(k==t){
						q=qi[j];
						qid=j;
						break;
					}
					k++;
				}
				QueryResult<BindingSet> rs=null;
				try {
					rs = store.execOpenRdf(q);
					System.out.println("Query"+qid+": "+(ExecutorOpenRdf.execTime-50));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(2);
				}
			}
	        System.exit(0);*/
	        
			int t = r.nextInt(b.cardinality());
			int k=0, qid=0;
			for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
				if(k==t){
					qid=j;
					q=qi[j];
					break;
				}
				k++;
			}
	        /*if(i%2==0)
				q=q4;*/

            output.write(q+"\n");
			System.out.println(q);
//			QueryResult<BindingSet> rs=null;
//			try {
//				rs = store.execOpenRdf(q);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				System.exit(2);
//			}
			
			/*Long ll1 = queryCount.get(qid);
			if(ll1==null){
				ll1=new Long(1);
				queryCount.put(qid, ll1);
				queryExecTime.put(qid, (ExecutorOpenRdf.execTime-50));
			}
			else{
				ll1++;
				Long l2 = queryExecTime.get(qid);
				l2+=(ExecutorOpenRdf.execTime-50);
			}*/

			//System.out.print((new Timestamp(System.currentTimeMillis())).toString()+"\t"+ExecutorOpenRdf.execTime);
			
//			if(!primitiveCostMap.containsKey(qid)){
//				primitiveCostMap.put(qid, 0.0);
//				costSavingMap.put(qid, 0.0);
//			}
//				
//			Double d = primitiveCostMap.get(qid);
//			d+=baselineExecTime.get(qid);
//			primitiveCostMap.put(qid, d);
//			d = costSavingMap.get(qid);
//			d+=(baselineExecTime.get(qid)-(ExecutorOpenRdf.execTime-50));
//			costSavingMap.put(qid, d);
//			
//			primitiveCost+=baselineExecTime.get(qid);
//			double save =baselineExecTime.get(qid)-(ExecutorOpenRdf.execTime-50);
//			//if(save>=0)
//			costSaving+=save;
//			
//			if(geomean.size()<10)
//				geomean.addLast((ExecutorOpenRdf.execTime-50));
//			else{
//				geomean.removeFirst();
//				geomean.addLast((ExecutorOpenRdf.execTime-50));
//			}
//			double mult =1, sum =0;
//			for(Long l1 : geomean){
//				mult*=(double)l1;
//				sum+=(double)l1;
//			}
//			//System.out.println(geomean);
//			double p = (double)1.0/(double)geomean.size();
//			//System.out.println(p);
//			double gm = Math.pow(mult, p);
//			double m =sum/(double)geomean.size();
//			double DCSR = (double)costSaving/(double)primitiveCost;
//			System.out.print((new Timestamp(System.currentTimeMillis())).toString()+"\t"+((System.currentTimeMillis()-start)/1000)+"\t"+DCSR+"\t{");
//			for(Entry<Integer,Double> e : primitiveCostMap.entrySet()){
//				double DCSRt = (double)costSavingMap.get(e.getKey())/(double)e.getValue();
//				System.out.print("<"+e.getKey()+": "+DCSRt+">,");
//			}
//			System.out.println("}");
			//System.out.println(((System.currentTimeMillis()-start)/1000)+"\t"+m+"\t"+gm);
		}
		
		/*for(Entry<Integer,Long> e : queryCount.entrySet()){
			double d = (double)queryExecTime.get(e.getKey())/(double)e.getValue();
			System.out.println("Query"+e.getKey()+": "+d);
		}*/
		
        /*String q2 = prolog + NL +
		"SELECT  ?x ?y ?z " +
		"WHERE   { " +
		"?x1 rdf:type ub:GraduateStudent ." +
		"?y rdf:type ub:University ." +
		"?z rdf:type ub:Department ." +
		"?x1 ub:memberOf ?z ." +
		"?z ub:subOrganizationOf ?y ." +
		"?x1 ub:undergraduateDegreeFrom ?y " +
		"}";
        String q=q2;
		QueryResult<BindingSet> rs=null;
		try {
			rs = store.execOpenRdf(q);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(2);
		}*/
		
		output.close();

		//store.close();
		
	}

}
