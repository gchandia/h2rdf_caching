package gr.ntua.h2rdf.coprocessors;

import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.partialJoin.ValueDoubleMerger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class ThreadedProxy extends Thread{
	
	HTable table;
	List<byte[]> list;
	byte[] row;
	HashMap<String, Long> index;
	
	public ThreadedProxy(HashMap<String, Long> index, HTable table, List<byte[]> list, byte[] row) {
		super("ThreadedProxy");
		this.index = index;
		this.table = table;
		this.list = list;
		this.row = row;
	}
	
	public void run() {
		TranslateIndexProtocol pr = table.coprocessorProxy(TranslateIndexProtocol.class, row);
	    List<byte[]> ret;
		try {
			ret = pr.translate(list);
			
		    Iterator<byte[]> itkey = list.iterator();
		    
		    if(ret==null){
		    	while(itkey.hasNext()){
			    	System.out.println(Bytes.toString(itkey.next()));
		    	}
		    }
		    
		    Iterator<byte[]> itvalue = ret.iterator();
		    while(itkey.hasNext()){
		    	SortedBytesVLongWritable v = new SortedBytesVLongWritable();
		    	byte[] temp = itvalue.next();
		    	if(temp.length==0){
		    		//System.out.println(Bytes.toString(itkey.next()));
		    		continue;
		    	}
		    	v.setBytesWithPrefix(temp);
		    	//System.out.println(Bytes.toString(itkey.next())+"\t"+v.getLong());
		    	index.put(Bytes.toString(itkey.next()), v.getLong());
		    }
	    	System.out.println("put: "+ret.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
		
	}
	
}
