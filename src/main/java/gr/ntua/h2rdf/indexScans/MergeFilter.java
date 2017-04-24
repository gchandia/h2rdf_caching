package gr.ntua.h2rdf.indexScans;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

public class MergeFilter implements Filter {
	private List<Scan> scans;
	
	public MergeFilter(List<Scan> scans) {
		this.scans =scans;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size =in.readInt();
		scans= new ArrayList<Scan>(size);
		for (int i = 0; i < size; i++) {
			Scan s = new Scan();
			s.readFields(in);
			scans.add(s);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(scans.size());
		for(Scan s : scans){
		    s.write(out);
		}
	}
	
	
	@Override
	public void reset() {
		
	}
	
	@Override
	public boolean filterAllRemaining() {
		return false;
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void filterRow(List<KeyValue> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean filterRowKey(byte[] arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public KeyValue getNextKeyHint(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasFilterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFamilyEssential(byte[] arg0) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public KeyValue transform(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
