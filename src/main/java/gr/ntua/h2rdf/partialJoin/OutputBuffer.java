package gr.ntua.h2rdf.partialJoin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

public class OutputBuffer extends OutputStream {

	FSDataOutputStream out;
	Path outFile;
	ByteArrayOutputStream outBuffer;
	boolean inMemory;
	FileSystem fs;
	
	public OutputBuffer(Path outFile, FileSystem fs) {
		System.out.println("Create Out Buffer");
		this.outFile = outFile;
		this.fs =fs;
		outBuffer =new ByteArrayOutputStream(500000); 
		inMemory=true;
	}

	public void writeBytes(String string) throws IOException {
		if(inMemory){
			outBuffer.write(Bytes.toBytes(string));
			if(outBuffer.size()>=500000){
				inMemory=false;
				if (fs.exists(outFile)) {
					fs.delete(outFile,true);
				}
		    	out = fs.create(outFile);
		    	outBuffer.flush();
		    	out.write(outBuffer.toByteArray());
			}
		}
		else{
			out.writeBytes(string);
		}
		
	}

	public int size() {
		if(inMemory){
			return outBuffer.size();
		}
		else{
			return out.size();
		}
	}

	public void close() throws IOException {
		if(inMemory){
			JoinPlaner.outputData = outBuffer.toByteArray();
			outBuffer.flush();
		}
		else{
			JoinPlaner.outputData = null;
			out.close();
		}
	}

	@Override
	public void write(int b) throws IOException {
		if(inMemory){
			outBuffer.write(b);
			if(outBuffer.size()>=500000){
				inMemory=false;
		    	out = fs.create(outFile);
		    	outBuffer.flush();
		    	out.write(outBuffer.toByteArray());
			}
		}
		else{
			out.write(b);
		}
	}
	
	

}
