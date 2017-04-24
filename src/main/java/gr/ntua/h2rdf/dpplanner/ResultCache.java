package gr.ntua.h2rdf.dpplanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ResultCache {
	
	public ResultCache() {
		// TODO Auto-generated constructor stub
	}
	
	public void initialize() {
		
	}
	
	public static void main(String[] args) {
		ResultCache cache = new ResultCache();
		
		HashMap<String,List<String>> query = new HashMap<String, List<String>>();
		
		List<String> l = new ArrayList<String>(); 
		l.add("2 ?y");
		l.add("4 ?n");
		l.add("7 ?l");
		query.put("?x", l);
		
		l = new ArrayList<String>(); 
		l.add("2 ?x");
		query.put("?y", l);
		
		l = new ArrayList<String>(); 
		l.add("4 ?x");
		query.put("?n", l);
		
		l = new ArrayList<String>(); 
		l.add("7 ?x");
		query.put("?l", l);
		
		
	}
}
