package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * An {@code IntAggregator} computes some aggregate value over a set of {@code IntField}s.
 */
public class IntAggregator implements Aggregator {

	/**
	 * Constructs an {@code Aggregate}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */
	
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;
	int i;
	HashMap<Field,Integer> aggregatedata;
    HashMap<Field,Integer> count;
    Field Tuplegbfield;

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		aggregatedata = new HashMap<Field, Integer>();
    	count = new HashMap<Field, Integer>();
	}
	
	public int data(){
		switch (what){
			case MIN: return Integer.MAX_VALUE;
			case MAX: return Integer.MIN_VALUE;
			case SUM: return 0;
			case COUNT: return 0;
			case AVG: return 0;
			default: return 0;
		}
	}

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		if (gbfield == Aggregator.NO_GROUPING)
			Tuplegbfield = null;
		else
			Tuplegbfield = tup.getField(gbfield);
		
		if (!aggregatedata.containsKey(Tuplegbfield)){
			aggregatedata.put(Tuplegbfield, data());
			count.put(Tuplegbfield, 0);

		}
		
		int tuplevalue = ((IntField) tup.getField(afield)).getValue();
    	int currentvalue = aggregatedata.get(Tuplegbfield);
    	int currentcount = count.get(Tuplegbfield);
    	int newvalue = currentvalue;
    	switch(what){
    		case MIN: 
    			if (tuplevalue > currentvalue)
    				newvalue = currentvalue;
    			else
    				newvalue = tuplevalue;
    				
    			break;
    			
    		case MAX:
    			if (tuplevalue > currentvalue)
    				newvalue = tuplevalue;
    			else
    				newvalue = currentvalue;
    			
    			break;
    			
    		case SUM:
    			count.put(Tuplegbfield, currentcount+1);
    			newvalue = tuplevalue + currentvalue;
    			break;
    			
    		case AVG:
    			count.put(Tuplegbfield, currentcount+1);
    			newvalue = tuplevalue + currentvalue;
    			break;
    			
    		case COUNT:
    			newvalue = currentvalue + 1;
    			break;
    			
    		default: break;
    	}
    	
    	aggregatedata.put(Tuplegbfield, newvalue);
	}
	
	public TupleDesc creategbTupleDesc()
    {
    	String[] names;
    	Type[] types;
    	if (gbfield == Aggregator.NO_GROUPING)
    	{
    		names = new String[] {"aggregatevalue"};
    		types = new Type[] {Type.INT_TYPE};
    	}
    	else
    	{
    		names = new String[] {"groupvalue", "aggregatevalue"};
    		types = new Type[] {gbfieldtype, Type.INT_TYPE};
    	}
    	return new TupleDesc(types, names);
    }

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The {@code aggregateVal} is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupledesc = creategbTupleDesc();
    	Tuple T;
    	for (Field group : aggregatedata.keySet())
    	{
    		int aggregateval;
    		if (what == Op.AVG)
    		{
    			aggregateval = aggregatedata.get(group) / count.get(group);
    		}
    		else
    		{
    			aggregateval = aggregatedata.get(group);
    		}
    		T = new Tuple(tupledesc);
    		if (gbfield == Aggregator.NO_GROUPING){
    			T.setField(0, new IntField(aggregateval));
    		}
    		else {
        		T.setField(0, group);
        		T.setField(1, new IntField(aggregateval));    			
    		}
    		tuples.add(T);
    	}
    	return new TupleIterator(tupledesc, tuples);
    
		//throw new UnsupportedOperationException("implement me");
	}

}
