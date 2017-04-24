package gr.ntua.h2rdf.coprocessors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import gr.ntua.h2rdf.indexScans.JoinPlan;

public interface TranslateIndexProtocol extends CoprocessorProtocol {
	
	public List<byte[]> translate(List<byte[]> list)
			throws IOException;

}
