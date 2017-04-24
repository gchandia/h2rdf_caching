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

import gr.ntua.h2rdf.client.*;

import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.datatypes.*;
import com.hp.hpl.jena.datatypes.xsd.impl.*;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Statement;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;


public class HbaseSequentialImportExample {

	public static void main(String[] args) {

		String address = "imas01.eu.archive.org";
		String table = "ARCOMEMDB_test2";
		String user = "arcomem";
		H2RDFConf conf = new H2RDFConf(address, table, user);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);
		store.setLoader("HBASE_SEQUENTIAL");
		
		String d0_0="http://www.Department0.University0.edu/";
		String ub ="http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
		String rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		String arco = "http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#";
		try{
			for (int i = 0; i < 10; i++) {
				Node s = Node.createURI(d0_0+"GraduateStudent"+i);
				//s = Node.createAnon();
				Node p = Node.createURI(rdf+"type");

	            RDFDatatype datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#double");
				Node o = Node.createLiteral(i+".78", "", datatype);
				Triple triple = Triple.create(s, p, o);
				store.add(triple);
				/*s = Node.createURI(d0_0+"GraduateStudent"+i);
				p = Node.createURI(rdf+"type");
				o = Node.createURI(ub+"GraduateStudent");
				triple = Triple.create(s, p, o);
				store.add(triple);*/
			}
		} catch (NotSupportedDatatypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		store.close();
	}

}
