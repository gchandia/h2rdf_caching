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

import org.apache.hadoop.hbase.util.Bytes;

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.JavaApiCall;

public class JavaApiCallExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String address = "ia200127.eu.archive.org";
		String table = "ARCOMEMDB";
		String user = "arcomem";
		H2RDFConf conf = new H2RDFConf(address, table, user);
		JavaApiCall call = new JavaApiCall(conf);
		byte[] value = Bytes.toBytes("Hello from client!!");
		//type of request != 0, reserved for SPARQL queries
		byte type = (byte) 4; 
		//serialized input of request
		byte[] b = new byte[value.length+1];
		b[0]=type;
		for (int i = 0; i < value.length; i++) {
			b[i+1]=value[i];
		}
		try {
			byte[] ret = call.send(b);
			System.out.println(Bytes.toString(ret));
			call.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally{
			call.close();
		}

	}

}
