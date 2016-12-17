package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * A {@code StringAggregator} computes some aggregate value over a set of {@code StringField}s.
 */
public class StringAggregator implements Aggregator {

	/**
	 * Constructs a {@code StringAggregator}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports {@code COUNT}
	 * @throws IllegalArgumentException
	 *             if {@code what != COUNT}
	 */
	
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;
	HashMap <Field,Integer> count;
	Field Tuplegbfield;

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		assert(what == Op.COUNT);
    	count = new HashMap<Field, Integer>();
	}

	/**
	 * Merges a new tuple into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		if (gbfield == Aggregator.NO_GROUPING)
			Tuplegbfield = null;
		else
			Tuplegbfield = tup.getField(gbfield);
    	
    	if (!count.containsKey(Tuplegbfield))
    	{
    		count.put(Tuplegbfield, 0);
    	}
    	
    	int currentcount = count.get(Tuplegbfield);
    	count.put(Tuplegbfield, currentcount+1);

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
	 *         or a single ({@code aggregateVal}) if no grouping. The aggregateVal is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupledesc = creategbTupleDesc();
    	Tuple T;
    	for (Field group : count.keySet())
    	{
    		int aggregateval = count.get(group);
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
