package gr.ntua.h2rdf.dpplanner;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SizePowerSet {
    private ArrayList<Integer> arr = null;
    private BitSet bset = null;
    private BitSet ret = null;
    private BitSet lowlevelBSet = null;
	private Iterator<Integer> oneLevelit;
	private int size, bsetsize;
	private SizePowerSet lowerSet;
	private int first, previousOneLevel;

    @SuppressWarnings("unchecked")
    public SizePowerSet(BitSet set, int size)
    {
    	this.size=size;
    	bset=set;
    	bsetsize = set.size();
    	arr = new ArrayList<Integer>();
        for (int i = 0; i <= set.size(); i++) {
        	if(set.get(i))
        		arr.add(i);
		}
        oneLevelit = arr.iterator();
    	if(size>1){
    		lowerSet = new SizePowerSet(set,size-1);
    	}
    	else if(size==0){
    		first=0;
    	}
    	previousOneLevel=0;
    }

    public BitSet next() {
    	if(size==1){
    		if(oneLevelit.hasNext()){
    	    	BitSet ret = new BitSet(bsetsize);
    			ret.set(oneLevelit.next());
    			return ret;
    		}
    		else{
    			return null;
    		}
    	}
    	else if(size>1){
    		if(oneLevelit.hasNext()){
    			if(lowlevelBSet==null){
    				lowlevelBSet= lowerSet.next();
    				if(lowlevelBSet==null)
    					return null;
    			}
    	    	//BitSet ret = (BitSet)lowlevelBSet.clone();
    			int t = oneLevelit.next();
				lowlevelBSet.clear(previousOneLevel);
    			int minLowlevel = lowlevelBSet.nextSetBit(0);
    			if(t<minLowlevel){
    				lowlevelBSet.set(t);
    				previousOneLevel=t;
    				return lowlevelBSet;
    			}
    			else{
        			//restart oneLevelBitSet
        	        oneLevelit = arr.iterator();
        	        //next lowlevel
        			lowlevelBSet = lowerSet.next();
        			previousOneLevel=0;
        			if(lowlevelBSet==null)
        				return null;
    				return next();
    			}
    		}

			lowlevelBSet = lowerSet.next();
			previousOneLevel=0;
			if(lowlevelBSet==null)
				return null;
    		else {
    			//restart oneLevelBitSet
    	        oneLevelit = arr.iterator();
    	        //next lowlevel
    			int t = oneLevelit.next();
    			int minLowlevel = lowlevelBSet.nextSetBit(0);
    			if(t<minLowlevel){
    				lowlevelBSet.set(t);
    				previousOneLevel=t;
    				return lowlevelBSet;
    			}
    			else{
        			//restart oneLevelBitSet
        	        oneLevelit = arr.iterator();
        	        //next lowlevel
        			lowlevelBSet = lowerSet.next();
        			previousOneLevel=0;
        			if(lowlevelBSet==null)
        				return null;
    				return next();
    			}
    		}
    	}
    	else if(size==0 && first==0){
    		first++;
    		return new BitSet(bsetsize);
    	}
    	return null;
    }

    public static void main(String[] args) {
    	HashMap<Integer, int[]> g = new HashMap<Integer,int[]>();
    	int[] l1 = {1,2};
    	int[] l2 = {1,3};
    	int[] l3 = {1,4};
    	int[] l4 = {2,4};
    	int[] l5 = {3,4};
    	g.put(1, l1);
    	g.put(2, l2);
    	g.put(3, l3);
    	g.put(4, l4);
    	g.put(5, l5);
    	
    	HashMap<Integer,Double> exp = new HashMap<Integer, Double>();
    	exp.put(1, new Double(0.8));
    	exp.put(2, new Double(0.5));
    	exp.put(3, new Double(0.6));
    	exp.put(4, new Double(0.7));
    	
    	long time =System.currentTimeMillis();
    	int n =5;
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}

		SizePowerSet p = new SizePowerSet(b, 3);
		BitSet b1;
		int count =0;
		while((b1 = p.next())!=null){
			List<BitSet> minb=new ArrayList<BitSet>();
			List<Double> mp1=new ArrayList<Double>(),mp2=new ArrayList<Double>(),mp3=new ArrayList<Double>();
			double minerror = Double.MAX_VALUE;
			System.out.println(b1);
			for (double p1 = 0.01; p1 < 1; p1+=0.01) {
				for (double p2 = 0.01; p2 < 1; p2+=0.01) {
					for (double p3 = 0.01; p3 < 1; p3+=0.01) {
						//System.out.print(b1+", "+ p1+ " "+ p2+ " "+ p3+" : ");

				    	HashMap<Integer,Double> expn = new HashMap<Integer, Double>();
				    	for (int i = 1; i <= 4; i++) {
				    		expn.put(i, new Double(0.0));
						}
						int k=0;
						for (int i = b1.nextSetBit(0); i >= 0; i = b1.nextSetBit(i+1)) {
							if(k==0){
								int[] e = g.get(i);
								expn.put(e[0],expn.get(e[0])+p1);
								expn.put(e[1],expn.get(e[1])+p1);
							}
							else if(k==1){

								int[] e = g.get(i);
								expn.put(e[0],expn.get(e[0])+p2);
								expn.put(e[1],expn.get(e[1])+p2);
							}
							else if(k==2){
								int[] e = g.get(i);
								expn.put(e[0],expn.get(e[0])+p3);
								expn.put(e[1],expn.get(e[1])+p3);
							}
							k++;
							//System.out.print(i + ", ");
						}
						double error=0;
						for (int i = 1; i <= 4; i++) {
				    		error+= Math.pow(exp.get(i)-expn.get(i), 2);
						}
						if(minerror>error+0.001){
							minerror=error;
							mp1=new ArrayList<Double>();
							mp2=new ArrayList<Double>();
							mp3=new ArrayList<Double>();
							minb = new ArrayList<BitSet>();
							mp1.add(p1);
							mp2.add(p2);
							mp3.add(p3);
							BitSet bt = new BitSet();
							bt.or(b1);
							minb.add(bt);
						}
//						else if(Math.abs(minerror-error)<0.001){
//							mp1.add(p1);
//							mp2.add(p2);
//							mp3.add(p3);
//							BitSet bt = new BitSet();
//							bt.or(b1);
//							minb.add(bt);
//						}
						//System.out.println( error);
					}
				}
			}
			for (int i = 0; i < mp1.size(); i++) {
				System.out.println(mp1.get(i)+" "+ mp2.get(i)+" "+ mp3.get(i)+" error: "+ minerror);
			}
			count++;
		}
		//System.out.println("Count: "+count);
		
		System.exit(0);
		/*
		
		int count =0;
		//for(int i = 0; i <= n; i++){
			int i=4;
			SizePowerSet p = new SizePowerSet(b, i);
			BitSet b1;
			while((b1 = p.next())!=null){
				System.out.println(b1);
				count++;
			}
		//}
		System.out.println("Count: "+count);
    	long stoptime =System.currentTimeMillis();
    	System.out.println("Time ms: "+(stoptime-time));*/
	}
}