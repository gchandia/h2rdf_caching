package gr.ntua.h2rdf.indexScans;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import gr.ntua.h2rdf.LoadTriples.ByteTriple;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class PartitionFinder {

	private Pair<byte[][], byte[][]> keys;

	public PartitionFinder(Pair<byte[][], byte[][]> keys) {
		this.keys = keys;
	}
	
	public long[][] getPartition(byte[] row, int pos) throws IOException{
		int numRegions =0;
		List<long[]> ret = new LinkedList<long[]>();
		for (int i = 0; i < keys.getFirst().length; i++) {
			int comp1=0, comp2 =0;
			if (!(keys.getFirst()[i] == null ||
			        keys.getFirst()[i].length == 0)) {
				comp1 = Bytes.compareTo(row, 0, row.length, keys.getFirst()[i], 0, row.length);
			}
			if (!(keys.getSecond()[i] == null ||
			        keys.getSecond()[i].length == 0)) {
				comp2 = Bytes.compareTo(row, 0, row.length, keys.getSecond()[i], 0, row.length);
			}
			if(comp1>=0 && comp2<=0){//used region
				long[] l = new long[2];
				if (keys.getFirst()[i] == null ||
				        keys.getFirst()[i].length == 0) {
					l[0]=Long.MIN_VALUE;
				}
				else{
					if(numRegions==0){
						l[0]=Long.MIN_VALUE;
					}
					else{
						long[] n = ByteTriple.parseRow(keys.getFirst()[i]);
						l[0]=n[pos];
					}
				}

				if (keys.getSecond()[i] == null ||
				        keys.getSecond()[i].length == 0) {
					l[1]=Long.MAX_VALUE;
				}
				else{
					long[] n = ByteTriple.parseRow(keys.getSecond()[i]);
					l[1]=n[pos]+1;
				}
				numRegions++;
				ret.add(l);
			}
			else{
				if(numRegions>0){
					ret.get(numRegions-1)[1]=Long.MAX_VALUE;
					break;
				}
			}
		}
		
		long[][] r = new long[numRegions][2];
		Iterator<long[]> it = ret.iterator();
		int i=0;
		while(it.hasNext()){
			r[i] = it.next();
			i++;
		}
		
		return r;
	}
	
}
