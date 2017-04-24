package gr.ntua.h2rdf.inputFormat2;

import java.io.IOException;
import java.util.List;

import gr.ntua.h2rdf.indexScans.Bindings;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordReader;

public interface TableRecordReaderInt {
	public List<Bindings> getCurrentKey();
	public BytesWritable getCurrentValue();
	public boolean nextKeyValue() throws IOException, InterruptedException;
	public boolean goTo(long k) throws IOException, InterruptedException;
	public void close() throws IOException;
	public Long getJvar();
}
