package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.bytes.H2RDFNode;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class IDTranslator {
	private static HashMap<String,HashMap<String, Long>> idCache= new HashMap<String, HashMap<String,Long>>();
	private static long curId=0;
	public static Long translate(String name, HTable indexTable) throws IOException{
		HashMap<String, Long> tablemap = idCache.get(Bytes.toString(indexTable.getTableName()));
		if(tablemap!=null){
			Long l = tablemap.get(name);
			if(l!=null)
				return l;
			else{
				
				Get get = new Get(Bytes.toBytes(name));
				get.addColumn(Bytes.toBytes("1"), new byte[0]);
				Result res = indexTable.get(get);
				SortedBytesVLongWritable v = new SortedBytesVLongWritable();
				if(res.isEmpty())
					throw new IOException("node:"+name+" not found");
				v.setBytesWithPrefix(res.value());
				tablemap.put(name, v.getLong());
				return v.getLong();
				/*curId++;
				tablemap.put(name, curId);
				return curId;*/
			}
		}
		else{
			tablemap = new HashMap<String, Long>();
			idCache.put(Bytes.toString(indexTable.getTableName()), tablemap);
			Get get = new Get(Bytes.toBytes(name));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node:"+name+" not found");
			v.setBytesWithPrefix(res.value());
			tablemap.put(name, v.getLong());
			return v.getLong();
			/*curId++;
			tablemap.put(name, curId);
			return curId;*/
		}
	}
}
