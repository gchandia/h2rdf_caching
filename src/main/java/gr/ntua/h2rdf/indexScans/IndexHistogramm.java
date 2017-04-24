package gr.ntua.h2rdf.indexScans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer.Context;

public class IndexHistogramm {
	private long[] key;
	private byte table;
	private List<HistogrammGap> gaps; //write gap when its complete
	private long currCount, currCountOther, lastKey, firstKey;
	private static final long MAX_ZERO_GAP = 1000;
	private static final long MAX_COUNT = 1000;
	private int type;
	
	public IndexHistogramm(byte table, long[] key, long firstKey, int type) {
		this.type=type;
		this.table = table;
		this.key = key;
		this.firstKey = firstKey;
		this.lastKey = firstKey;
		currCount = 1;
		currCountOther = 0;
		gaps = new ArrayList<HistogrammGap>();
	}
	
	public void addKey(long key, long countOtherPrev){
		if(key-lastKey>MAX_ZERO_GAP){
			//add previous non zero gap
			if(currCount>0)
				gaps.add(new HistogrammGap(lastKey, lastKey-firstKey, currCount, currCountOther+countOtherPrev));
			//restart
			firstKey = key;
			lastKey = key;
			currCount = 1;
			currCountOther = 0;
			return;
		}
		else{// no zero gap
			if(currCount<MAX_COUNT){// add key to current gap
				lastKey = key;
				currCount++;
				currCountOther +=countOtherPrev;
				return;
			}
			else{//write gap and create new
				gaps.add(new HistogrammGap(lastKey, lastKey-firstKey, currCount, currCountOther+countOtherPrev));
				firstKey = key;
				lastKey = key;
				currCount = 1;
				currCountOther = 0;
				return;
			}	
		}
	}
	
	public void addLastCount(long countOtherPrev){
		gaps.add(new HistogrammGap(lastKey, lastKey-firstKey, currCount, currCountOther+countOtherPrev));
	}
	
	public void write(Context context){
		Iterator<HistogrammGap> it = gaps.iterator();
		while(it.hasNext()){
			HistogrammGap t = it.next();
			t.write(context, key, table, type);
		}
	}
}
