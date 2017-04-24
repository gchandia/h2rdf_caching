package gr.ntua.h2rdf.byteImport;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class createData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try{
			  // Create file 
			  int max_univ = Integer.parseInt(args[0]);
			  int max_dep = Integer.parseInt(args[1]);
			  int max_stud = Integer.parseInt(args[2]);
			  int num =((max_univ-10000)*max_dep*max_stud);
			  FileWriter fstream = new FileWriter(num+"C");
			  BufferedWriter out = new BufferedWriter(fstream);
			  //System.out.println(max_univ+" "+max_dep+" "+max_stud);
			  for (int univ = 10000; univ < max_univ; univ++) {
				  for (int dep = 0; dep < max_dep; dep++) {
					  for (int stud = 0; stud < max_stud; stud++) {
						  out.write("<http://www.Department"+dep+".University"+univ+".edu/Course"+stud+"> " +
							  		"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
							  		"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course"+num+"> .");
						  out.newLine();
						  out.write("<http://www.Department"+dep+".University"+univ+".edu/GraduateCourse"+stud+"> " +
							  		"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
							  		"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse"+num+"> .");
						  out.newLine();
					  }
				  }
			  }
			  //Close the output stream
			  out.close();
		}catch (Exception e){//Catch exception if any
			e.printStackTrace();
	  	}

	}

}
