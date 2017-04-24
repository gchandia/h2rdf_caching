package gr.ntua.h2rdf.LoadTriples;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class Pair implements Comparable<Pair> {

	private Integer key;
	private ImmutableBytesWritable value;
	
	public Pair(int key, ImmutableBytesWritable value) {
		this.key =key;
		this.value = value;
	}
	public Integer getKey() {
		return key;
	}
	public void setKey(Integer key) {
		this.key = key;
	}
	public ImmutableBytesWritable getValue() {
		return value;
	}
	public void setValue(ImmutableBytesWritable value) {
		this.value = value;
	}
	
	@Override
	public int compareTo(Pair o) {
		Integer k = o.getKey();
		ImmutableBytesWritable v = o.getValue();
		int r = k.compareTo(key);
		if(r!=0){
			return r;
		}
		return v.compareTo(value);
	}

}
