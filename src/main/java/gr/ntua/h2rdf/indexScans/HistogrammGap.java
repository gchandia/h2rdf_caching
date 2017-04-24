package gr.ntua.h2rdf.indexScans;

import java.io.IOException;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;
import gr.ntua.h2rdf.LoadTriples.SortedBytesVLongWritable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HistogrammGap {
	private long key, gap;
	private long countJoin, countOther;
	
	public HistogrammGap(long key, long gap, long countJoin, long countOther) {
		this.key = key;
		this.gap = gap;
		this.countJoin = countJoin;
		this.countOther = countOther;
	}

	public void write(Context context, long[] key, byte table, int type) {
		byte[] k = ByteTriple.createByte(key[0], key[1], key[2], table);
		String f ="";
		switch (type) {
		case 1:
			f = "A";
			break;
		case 2:
			f = "B";
			break;
		case 3:
			f = "C";
			break;

		default:
			//error 
			System.out.print("wrong length");
			System.exit(15);
			break;
		}
		SortedBytesVLongWritable s = new SortedBytesVLongWritable();
		KeyValue emmitedValue = null;
		if(!f.equals("C")){
			System.out.println("Key: "+ Bytes.toStringBinary(k)+" fam: "+f +" countJoin: "+countJoin+" countOther: "+countOther);
			emmitedValue = new KeyValue(k, Bytes.toBytes(f), (new SortedBytesVLongWritable(countJoin)).getBytesWithPrefix() 
				, (new SortedBytesVLongWritable(countOther)).getBytesWithPrefix());
		}
		else{
			System.out.println("Key: "+ Bytes.toStringBinary(k)+" fam: "+f +" countJoin: "+countJoin);
			emmitedValue = new KeyValue(k, Bytes.toBytes(f), (new SortedBytesVLongWritable(countJoin)).getBytesWithPrefix() 
					, null);
		}
			
		
    	try {
    		context.write(new ImmutableBytesWritable(k), emmitedValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}
