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
package com.hp.hpl.jena.sparql.algebra;


import java.util.Iterator;
import java.util.List;
import java.util.Set;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import gr.ntua.h2rdf.partialJoin.JoinPlaner;
import gr.ntua.h2rdf.partialJoin.OutputBuffer;
import gr.ntua.h2rdf.partialJoin.QueryProcessor;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.PatternVars;


public class MyOpVisitor extends OpVisitorBase {

	private Query query;
	private String id;
	private OpBGP opBGP;
	private boolean ask;
	
	public MyOpVisitor(String id, Query query) {
		this.query = query;
		this.id = id;
		ask =true;
	}
	
    public void visit(OpBGP opBGP)
    {
    	System.out.println("bgp");
    	System.out.println(opBGP.toString());
    	this.opBGP = opBGP;
    }

    public void visit(OpJoin opJoin)
    {
    	System.out.println("join");
    	System.out.println(opJoin.toString());}

    public void visit(OpLeftJoin opLeftJoin)
    {
    	System.out.println("left join");
    	System.out.println(opLeftJoin.toString());}

    public void visit(OpUnion opUnion)
    {
    	System.out.println("union");
    	System.out.println(opUnion.toString());}

    public void visit(OpFilter opFilter)
    {
    	System.out.println("filter");
    	System.out.println(opFilter.toString());
    	/*//JoinPlanner.setFilterVars();
    	Iterator<Expr> it = opFilter.getExprs().iterator();
    	while(it.hasNext()){
    		Expr e =it.next();
    		Iterator<Expr> a = e.getFunction().getArgs().iterator();
			System.out.println(e.getFunction().getOpName());
    		while(a.hasNext()){
    			Expr temp = a.next();
    			if(temp.isVariable())
    				JoinPlaner.filter(temp.toString(),e.getFunction());
    		}
    	}*/
    }

    public void visit(OpGraph opGraph)
    {
    	System.out.println("graph");
    	System.out.println(opGraph.toString());}

    public void visit(OpQuadPattern quadPattern)
    {
    	System.out.println("quad");
    	System.out.println(quadPattern.toString());}

    public void visit(OpDatasetNames dsNames)
    {
    	System.out.println("dsNames");
    	System.out.println(dsNames.toString());}

    public void visit(OpTable table)
    {
    	System.out.println("table");
    	System.out.println(table.toString());}

    public void visit(OpExt opExt)
    {
    	System.out.println("ext");
    	System.out.println(opExt.toString());}

    public void visit(OpOrder opOrder)
    {
    	System.out.println("order");
    	System.out.println(opOrder.toString());}

    public void visit(OpProject opProject)
    {
    	System.out.println("project");
    	System.out.println(opProject.toString());
    	ask = false;
    }

    public void visit(OpDistinct opDistinct)
    {
    	System.out.println("distinct");
    	System.out.println(opDistinct.toString());}

    public void visit(OpSlice opSlice)
    {
    	System.out.println("slice");
    	System.out.println(opSlice.toString());}
    
    public void execute(){

		Configuration conf = new Configuration();
    	FileSystem fs = null;
    	try {
    		fs = FileSystem.get(conf);
    		Path out =new Path("output");
    		if(!fs.exists(out)){
    			fs.delete(out,true);
    			fs.mkdirs(out);
    		}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	Triple[] Q= new Triple[0];
    	Q = opBGP.getPattern().getList().toArray(Q);
    	Set<Var> vars = PatternVars.vars(query.getQueryPattern());
    	
    	JoinPlaner.setid(id);
    	JoinPlaner.newVaRS(vars);
    	try {
			JoinPlaner.form(Q);
	    	JoinPlaner.removeNonJoiningVaribles(Q);
	    	int i=0;
	    	while(!JoinPlaner.isEmpty()){
	    		String v = JoinPlaner.getNextJoin();
	    		System.out.println(v);
	    		i++;
	    	}
	    	if(i==0 ){
	    		Path outFile=new Path("output/Join_"+id+"_"+0);
	    		OutputBuffer out = new OutputBuffer(outFile, fs);
				//if (fs.exists(outFile)) {
				//	fs.delete(outFile,true);
				//}
	    		//fs.create(outFile);
				QueryProcessor.executeSelect(Q[0], out, "P0");
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
}
