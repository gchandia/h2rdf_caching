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
package gr.ntua.h2rdf.partialJoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.JenkinsHash;

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.H2RDFNode;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

import javax.activation.UnsupportedDataTypeException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;

import gr.ntua.h2rdf.byteImport.MyNewTotalOrderPartitioner;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.expr.ExprFunction;


public class QueryProcessor {
	private static Node s;
	private static Node p;
	private static Node o;
	private static Boolean isVariableS;
	private static Boolean isVariableP;
	private static Boolean isVariableO;
	private static HTable table;
	private static final int totsize= ByteValues.totalBytes, rowlength=1+2*totsize;
	
	public static void executeSelect(Triple bgp, OutputBuffer out, String bgpNo) throws NotSupportedDatatypeException {
		try {
			table = new HTable(HBaseConfiguration.create(), JoinPlaner.getTable());
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println(bgp);
		processBgp(bgp);
		executeSpecifSelect(bgp, out, numVars(), bgpNo);	
	}

	public static Scan getScan(Triple q2) throws Exception {
		//System.out.println(q2);
		processBgp(q2);
		return getSpecifScan(q2, numVars());	
	}


	private static Scan getSpecifScan(Triple q2, int numVars) throws Exception {
		Scan scan=null;
		String t = getTable();
		
		
		byte p=(byte)0;
		if(t.equals("spo"))
			p=(byte)4;
		else if(t.equals("pos"))
			p=(byte)3;
		else if(t.equals("osp"))
			p=(byte)2;
		byte[] row=null;
		byte[] col=null;
		byte[] startfilter=new byte[ByteValues.totalBytes];
		byte[] stopfilter=new byte[ByteValues.totalBytes];
		byte[] startfilter0=new byte[ByteValues.totalBytes];
		byte[] stopfilter0=new byte[ByteValues.totalBytes];
		for (int i = 0; i < stopfilter.length; i++) {
			startfilter[i]=(byte) 0;
			stopfilter[i]=(byte) 255;
			startfilter0[i]=(byte) 0;
			stopfilter0[i]=(byte) 255;
		}
		if(numVars==0){
			
			scan = new Scan();
			row= new byte[1+2*ByteValues.totalBytes];
			row[0]=p;
			byte[] bid=getByteId(t,0);
			
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				row[i+1]=bid[i];
			}
			bid=getByteId(t,1);
			//bid=Bytes.toBytes(colidt);
			//System.out.println(colidt+"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				row[i+ByteValues.totalBytes+1]=bid[i];
			}

			scan.addColumn(Bytes.toBytes("A"), getByteId(t,2));
			System.out.println(Bytes.toStringBinary(row));
			scan.setStartRow(row);
		}
		else if(numVars==1){
			System.out.println(t);
			if(JoinPlaner.filters.containsKey(getVariable(t.charAt(2))) ){
				System.out.println(getVariable(t.charAt(2)));
				System.out.println(JoinPlaner.filters.get(getVariable(t.charAt(2))));
			}
			scan = new Scan();
			row= new byte[1+2*ByteValues.totalBytes];
			row[0]=p;
			byte[] bid=getByteId(t,0);
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				row[i+1]=bid[i];
			}
			bid=getByteId(t,1);
			//bid=Bytes.toBytes(colidt);
			//System.out.println(colidt+"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				row[i+ByteValues.totalBytes+1]=bid[i];
			}
			System.out.println(Bytes.toStringBinary(row));
			scan.setStartRow(row);
		}
		else if(numVars==2){
			if(JoinPlaner.filters.containsKey(getVariable(t.charAt(2))) ){//low performance
				System.out.println(getVariable(t.charAt(2)));
				System.out.println(JoinPlaner.filters.get(getVariable(t.charAt(2))));
			}
			if(JoinPlaner.filters.containsKey(getVariable(t.charAt(1))) ){
				System.out.println(getVariable(t.charAt(1)));
				Iterator<ExprFunction> l = JoinPlaner.filters.get(getVariable(t.charAt(1))).iterator();
				System.out.println(Bytes.toStringBinary(startfilter));
				System.out.println(Bytes.toStringBinary(stopfilter));
				while(l.hasNext()){
					ExprFunction filter = l.next();
					if(filter.getVarsMentioned().size()==1){
						ProcessFilters.process(startfilter, stopfilter, filter);
						System.out.println(Bytes.toStringBinary(startfilter));
						System.out.println(Bytes.toStringBinary(stopfilter));
					}
					else{
						throw new Exception("usupported filter type"+filter.toString());
					}
				}
				//System.out.println(JoinPlaner.filters.get(getVariable(t.charAt(1))));
			}
			if(Bytes.equals(startfilter, startfilter0) && Bytes.equals(stopfilter, stopfilter0)){
				scan = new Scan();
				row= new byte[1+ByteValues.totalBytes];
				row[0]=p;
				byte[] bid=getByteId(t,0);
				for (int i = 0; i < ByteValues.totalBytes; i++) {
					row[i+1]=bid[i];
				}
				System.out.println(Bytes.toStringBinary(row));
				scan.setStartRow(row);
				scan.setStopRow(row);
				byte[] a = Bytes.toBytes(getVariable(t.charAt(1))+"|"+getVariable(t.charAt(2)));
				col = new byte[a.length];
				for (int i = 0; i < a.length; i++) {
					col[i]=a[i];
				}
				System.out.println(Bytes.toStringBinary(col));
				scan.addFamily(col);
			}
			else{
				scan = new Scan();
				byte[] start= new byte[1+2*ByteValues.totalBytes];
				byte[] stop= new byte[1+2*ByteValues.totalBytes];
				start[0]=p;
				stop[0]=p;
				byte[] bid=getByteId(t,0);
				for (int i = 0; i < ByteValues.totalBytes; i++) {
					start[i+1]=bid[i];
					stop[i+1]=bid[i];
				}
				for (int i = 0; i < ByteValues.totalBytes; i++) {
					start[i+ByteValues.totalBytes+1]=startfilter[i];
					stop[i+ByteValues.totalBytes+1]=stopfilter[i];
				}
				System.out.println(Bytes.toStringBinary(row));
				scan.setStartRow(start);
				scan.setStopRow(stop);
				
				byte[] a = Bytes.toBytes(getVariable(t.charAt(1))+"|"+getVariable(t.charAt(2)));
				col = new byte[a.length];
				for (int i = 0; i < a.length; i++) {
					col[i]=a[i];
				}
				System.out.println(Bytes.toStringBinary(col));
				scan.addFamily(col);
			}
		}
		else if(numVars==3){
			scan = new Scan();
			row= new byte[1+2*ByteValues.totalBytes];
			row[0]=(byte) 3;
			scan.setStartRow(row);	
		}
		return scan; 
	}

	private static byte[] getByteId(String table, int pos) throws Exception {
		H2RDFNode node = null;
		if(table.charAt(pos)=='s'){
			node = new H2RDFNode(s);
		}
		else if(table.charAt(pos)=='p'){
			node = new H2RDFNode(p);
		}
		else if(table.charAt(pos)=='o'){
			node = new H2RDFNode(o);
		}
		return node.getHashValue();
		
		/*byte[] ret = ByteValues.getFullValue(string);
		if (ret.length==ByteValues.totalBytes)
			return ret;
		else 
			throw new Exception("wrong id length");
		*/
	}
	/*private static byte[] getRowIdbyte(String table) throws Exception {
		String string ="";
		if(table.startsWith("s")){
			string="<"+s+">";
		}
		else if(table.startsWith("p")){
			string="<"+p+">";
		}
		else if(table.startsWith("o")){
			if(!o.contains("^^"))
				string="<"+o+">";
			else{
				StringTokenizer tok =new StringTokenizer(o);
				String v=tok.nextToken("^^");
				string +="\""+v+"\"^^<"+tok.nextToken("^^")+">";
			}
		}
		System.out.println(string);
		
		byte[] ret = ByteValues.getFullValue(string);
		if (ret.length==ByteValues.totalBytes)
			return ret;
		else 
			throw new Exception("wrong id length");
		
	}*/

	public static String getInpVars() {
		int numVars =numVars();
		String ret ="";
		if(numVars==0){
			
		}
		else if(numVars==1){
			String t = getTable();
        	String var1=getVariable(t.charAt(2));
        	ret+=var1;
		}
		else if(numVars==2){
			String t = getTable();
        	String var1=getVariable(t.charAt(1));
        	String var2=getVariable(t.charAt(2));
        	ret+=var1+" ";
        	ret+=var2+" ";
		}
		else if(numVars==3){
        	ret+=s+" ";
        	ret+=p+" ";
        	ret+=o+" ";
		}
		return ret; 
	}
	
	private static int numVars() {
		int ret=0;
		if(isVariableS)
			ret++;
		if(isVariableP)
			ret++;
		if(isVariableO)
			ret++;
		return ret;
	}

	private static void executeSpecifSelect( Triple bgp, OutputBuffer out, int numVars, String pat) throws NotSupportedDatatypeException {
		
		byte[] rowid =JoinPlaner.getScan(0).getStartRow();
		byte[] startr = new byte[rowlength+2];
		byte[] stopr = new byte[rowlength+2];
		if(numVars==0){
			startr[0] =rowid[0];
			stopr[0] =rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				startr[i] =rowid[i];
				stopr[i] =rowid[i];
			}
			startr[startr.length-2] =(byte)0;
			startr[startr.length-1] =(byte)0;
			stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			Scan scan = JoinPlaner.getScan(0);
			scan.setStartRow(startr);
			scan.setStopRow(stopr);
			scan.setCaching(70000);
			scan.setCacheBlocks(true);
			ResultScanner resultScanner=null;
			try {
				resultScanner = table.getScanner(scan);
				Result re;
				while((re = resultScanner.next())!=null){
					if(re.size()!=0){
						out.write(Bytes.toBytes(pat+"!?bool#2|1_!\n"));
						return;
					}
				}
				out.write(Bytes.toBytes(pat+"!?bool#2|0_!\n"));
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				resultScanner.close();  // always close the ResultScanner!
			}
		}
		if(numVars==1){

			startr[0] =rowid[0];
			stopr[0] =rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				startr[i] =rowid[i];
				stopr[i] =rowid[i];
			}
			if (rowid.length==rowlength) {
				byte table = rowid[0];
				byte[] TYPE = (new H2RDFNode(Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))).getHashValue();
				byte[] objid = new byte[totsize];
				for (int i1 = 0; i1 < totsize; i1++) {
					objid[i1]=rowid[i1+totsize+1];
				}
				byte[] predid = new byte[totsize];
				for (int i1 = 0; i1 < totsize; i1++) {
					predid[i1]=rowid[i1+1];
				}
				if((table == (byte)3) && Bytes.equals(predid, TYPE)){
					scanSubclass(objid, out, pat);
					try {
						out.flush();
						out.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return;
				}
				
				startr[startr.length-2] =(byte)0;
				startr[startr.length-1] =(byte)0;
				stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
		}
		else if(numVars==2){
			if (rowid.length==totsize+1) {
				startr[0] =rowid[0];
				stopr[0] =rowid[0];
				for (int i = 1; i < rowid.length; i++) {
					startr[i] =rowid[i];
					stopr[i] =rowid[i];
				}
				  for (int i = totsize+1; i < startr.length-2; i++) {
					  startr[i] =(byte)0;
					  stopr[i] =(byte)255;
				  }
				  startr[startr.length-2] =(byte)0;
				  startr[startr.length-1] =(byte)0;
				  stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				  stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
			else{
				byte[] stop = JoinPlaner.getScan(0).getStopRow();
				startr[0] =rowid[0];
				stopr[0] =stop[0];
				for (int i = 1; i < 1+2*totsize; i++) {
					startr[i] =rowid[i];
					stopr[i] =stop[i];
				}
				startr[startr.length-2] =(byte)0;
				startr[startr.length-1] =(byte)0;
				stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
		}
		else if(numVars==3){
			startr[0] =rowid[0];
			stopr[0] =rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				startr[i] =(byte)0;
				stopr[i] =(byte)255;
			}
			startr[startr.length-2] =(byte)0;
			startr[startr.length-1] =(byte)0;
			stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			Scan scan = JoinPlaner.getScan(0);
			scan.setStartRow(startr);
			scan.setStopRow(stopr);
			scan.setCaching(70000);
			scan.setCacheBlocks(true);
			ResultScanner resultScanner=null;
			try {
				resultScanner = table.getScanner(scan);
				Result re;
				while((re = resultScanner.next())!=null){
					StringTokenizer vtok2 = new StringTokenizer(getInpVars());
					String v_s=vtok2.nextToken();
					String v_p=vtok2.nextToken();
					String v_o=vtok2.nextToken();

					if(re.size()!=0){
						Iterator<KeyValue> it = re.list().iterator();
						while(it.hasNext()){
							KeyValue kv = it.next();
							if(kv.getQualifier().length==totsize){
								  byte[] r = kv.getRow();
								  byte[] r1= new byte[totsize];
								  byte[] r2= new byte[totsize];
								  for (int j = 0; j < r1.length; j++) {
									  r1[j]=r[1+j];
								  }
								  for (int j = 0; j < r2.length; j++) {
									  r2[j]=r[totsize+1+j];
								  }
								  out.write(Bytes.toBytes(pat+"!"+v_p+"#"+ByteValues.getStringValue(r1)+"_!"+ 
										  v_o+"#"+ByteValues.getStringValue(r2)+"_!"+ 
									v_s+"#"+ByteValues.getStringValue(kv.getQualifier())+"_!\n"));
							}
						}
					}
				}
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				resultScanner.close();  // always close the ResultScanner!
			}
		}
		byte[] bid,a;
		a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}
		System.out.println("startr"+Bytes.toStringBinary(startr));
		System.out.println("stopr"+Bytes.toStringBinary(stopr));
		Scan scan =new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		
		//must be changed
		//scan.addFamily(bid);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result result = null;
			while((result=resultScanner.next())!=null){
				//System.out.println("size: "+result.size());
				Iterator<KeyValue> it = result.list().iterator();
				while(it.hasNext()){
					KeyValue kv = it.next();
					if(kv.getQualifier().length==totsize){
						if(numVars==1){
							  StringTokenizer vtok2 = new StringTokenizer(getInpVars());
							  String v1=vtok2.nextToken();
							  out.write(Bytes.toBytes(pat+"!"+v1+"#"+ByteValues.getStringValue(kv.getQualifier())+"_\n"));
						}
						else if(numVars==2){
							  StringTokenizer vtok2 = new StringTokenizer(getInpVars());
							  String v1=vtok2.nextToken();
							  String v2=vtok2.nextToken();
							  byte[] r = kv.getRow();
							  byte[] r1= new byte[totsize];
							  for (int j = 0; j < r1.length; j++) {
								  r1[j]=r[totsize+1+j];
							  }
							  out.write(Bytes.toBytes(pat+"!"+v1+"#"+ByteValues.getStringValue(r1)+"_!"+ 
								v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_!\n"));
						}
					}
					
				}
				
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
	}

	private static void scanSubclass(byte[] objid, OutputBuffer out, String pat) throws NotSupportedDatatypeException {
		byte[] TYPE = (new H2RDFNode(Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))).getHashValue();
		byte[] classrowStart = new byte[rowlength+2];
		byte[] classrowStop = new byte[rowlength+2];
		classrowStart[0]=(byte)3; //pos
		for (int i1 = 0; i1 < totsize; i1++) {
			classrowStart[i1+1]=TYPE[i1];
		}
		for (int i1 = 0; i1 < totsize; i1++) {
			classrowStart[i1+totsize+1]=objid[i1];
		}
		for (int i1 = 0;  i1< classrowStart.length-1; i1++) {
			classrowStop[i1]=classrowStart[i1];
		}
		
		classrowStart[classrowStart.length-2] = (byte) 0;
		classrowStart[classrowStart.length-1] = (byte) 0;
		classrowStop[classrowStop.length-2] = (byte) 255;
		classrowStop[classrowStop.length-1] = (byte) 255;
		byte[] a=Bytes.toBytes("A");
		byte[] bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}
		System.out.println("startr"+Bytes.toStringBinary(classrowStart));
		System.out.println("stopr"+Bytes.toStringBinary(classrowStop));
		Scan scan =new Scan();
		scan.setStartRow(classrowStart);
		scan.setStopRow(classrowStop);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		
		//must be changed
		//scan.addFamily(bid);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result result = null;
			while((result=resultScanner.next())!=null){
				//System.out.println("size: "+result.size());
				Iterator<KeyValue> it = result.list().iterator();
				while(it.hasNext()){
					KeyValue kv = it.next();
					if(kv.getQualifier().length==totsize){
						  StringTokenizer vtok2 = new StringTokenizer(getInpVars());
						  String v1=vtok2.nextToken();
						  out.write(Bytes.toBytes(pat+"!"+v1+"#"+ByteValues.getStringValue(kv.getQualifier())+"_\n"));
					}
					
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
		
		System.out.println("Subclass");
		byte[] SUBCLASS = (new H2RDFNode(Node.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf"))).getHashValue();
		
		classrowStart = new byte[rowlength+2];
		classrowStop = new byte[rowlength+2];
		classrowStart[0]=(byte)3; //pos
		for (int i1 = 0; i1 < totsize; i1++) {
			classrowStart[i1+1]=SUBCLASS[i1];
		}
		for (int i1 = 0; i1 < totsize; i1++) {
			classrowStart[i1+totsize+1]=objid[i1];
		}
		for (int i1 = 0;  i1< classrowStart.length-1; i1++) {
			classrowStop[i1]=classrowStart[i1];
		}
		
		classrowStart[classrowStart.length-2] = (byte) 0;
		classrowStart[classrowStart.length-1] = (byte) 0;
		classrowStop[classrowStop.length-2] = (byte) 255;
		classrowStop[classrowStop.length-1] = (byte) 255;
		
		
		a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i1 = 0; i1 < a.length; i1++) {
			bid[i1]=a[i1];
		}
		Scan scan1 =new Scan();
		scan1.setStartRow(classrowStart);
		scan1.setStopRow(classrowStop);
		scan1.setCaching(70000);
		scan1.setCacheBlocks(true);
		scan1.addFamily(bid);
		resultScanner=null;
		try {
			resultScanner = table.getScanner(scan1);
			Result result = null;
			while((result=resultScanner.next())!=null){
				System.out.println("Subclasses: "+result.size());
				Iterator<KeyValue> it = result.list().iterator();
				while(it.hasNext()){
					KeyValue kv = it.next();
					byte[] qq = kv.getQualifier();
					scanSubclass(qq, out, pat);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
		
		
		
	}

	private static int getPosition(String s, Object[] st) {
		int j=-1;
    	for (int i = 0; i < st.length; i++) {
			if(s.equals(st[i].toString())){
				j=i;
				break;
			}
		}
		return j;
	}

	private static String getVariable(char c) {
		if(c=='s')
			return s.toString();
		else if(c=='p')
			return p.toString();
		else if(c=='o')
			return o.toString();
		else
			return null;
	}

	
	/*private static byte[] getColId(String table) throws Exception {
		String string ="";
		if(table.charAt(1)=='s'){
			string="<"+s+">";
		}
		else if(table.charAt(1)=='p'){
			string="<"+p+">";
		}
		else if(table.charAt(1)=='o'){
			if(!o.contains("^^"))
				string="<"+o+">";
			else{
				StringTokenizer tok =new StringTokenizer(o);
				String v=tok.nextToken("^^");
				string +="\""+v+"\"^^<"+tok.nextToken("^^")+">";
			}
		}
		System.out.println(string);
		
		byte[] ret = ByteValues.getFullValue(string);
		if (ret.length==ByteValues.totalBytes)
			return ret;
		else 
			throw new Exception("wrong id length");
	}
	
	private static byte[] getColId2(String table) throws Exception {
		String string ="";
		if(table.charAt(2)=='s'){
			string="<"+s+">";
		}
		else if(table.charAt(2)=='p'){
			string="<"+p+">";
		}
		else if(table.charAt(2)=='o'){
			if(!o.contains("^^"))
				string="<"+o+">";
			else{
				StringTokenizer tok =new StringTokenizer(o);
				String v=tok.nextToken("^^");
				string +="\""+v+"\"^^<"+tok.nextToken("^^")+">";
			}
		}
		System.out.println(string);
		
		byte[] ret = ByteValues.getFullValue(string);
		if (ret.length==ByteValues.totalBytes)
			return ret;
		else 
			throw new Exception("wrong id length");
	}*/
	
	private static String getTable() {
		if(!isVariableS && !isVariableP && !isVariableO)
			return "spo";
		else if(isVariableS && !isVariableP && !isVariableO)
			return "pos";
		else if(!isVariableS && isVariableP && !isVariableO)
			return "osp";
		else if(!isVariableS && !isVariableP && isVariableO)
			return "spo";
		else if(isVariableS && isVariableP && !isVariableO)
			return "osp";
		else if(!isVariableS && isVariableP && isVariableO)
			return "spo";
		else if(isVariableS && !isVariableP && isVariableO)
			return "pos";
		else
			return "spo";
	}
	
	private static void processBgp(Triple t) {
		s=t.getSubject();
		if(t.getSubject().isVariable()){
			isVariableS = true;
		}
		else{
			isVariableS = false;
		}
		p=t.getPredicate();
		if(t.getPredicate().isVariable()){
			isVariableP = true;
		}
		else{
			isVariableP = false;
		}
		o=t.getObject();
		if(t.getObject().isVariable()){
			isVariableO = true;
		}
		else{
			isVariableO = false;
		}
		System.out.println(s+" "+p+" "+o);
		System.out.println(isVariableS+" "+isVariableP+" "+isVariableO);
	}

	

	public static void executeJoin(Path outFile, Object[] join_files, Configuration joinConf) {
		int m_rc = 0;
		String[] args=new String[join_files.length+1];
    	args[0]=outFile.getName();
		for (int i = 0; i < join_files.length; i++) {
			//System.out.println(join_files[i]);
			String[] temp = join_files[i].toString().split(":");
			if(join_files[i].toString().contains("BGP")){
				args[i+1]=temp[0]+":"+temp[1];
			}
			else{
				args[i+1]=temp[0];
			}
			//args[i+1]=join_files[i].toString().split(":")[0];
		}
        try {
			m_rc = ToolRunner.run(joinConf ,new HbaseJoinBGP(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(5);
		}
		
	}

}
