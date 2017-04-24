package gr.ntua.h2rdf.LoadTriples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class LoadTriples {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			if(args.length!=2){
				System.out.println("wrong input");
				System.out.println("expected two arguments\n1)dataset path in hdfs\n2)database name");
				return;
			}
			//Create Index
			Configuration conf = new Configuration();
			ToolRunner.run(conf, new DistinctIds(), args);
			//Translate triples and create hexastore index
			conf = new Configuration();
			ToolRunner.run(conf, new TranslateAndImport(), args);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}

}
