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
	protected Op what;
	protected int gbfield;
	protected Type gbfieldtype;
	protected int afield;
	protected int i;
	protected HashMap<Field,Integer> aggHashmap;
	//hashmap containing totals
    protected HashMap<Field,Integer> total;
    protected Field myGbfield;
    //the int aggregator
	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) { 
		// intAggregator constructor
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		aggHashmap = new HashMap<Field, Integer>();
    	total = new HashMap<Field, Integer>();
	}
	
	//method which grab the op value and returns the correct integer
	public int traverseObj(){
		if(what == Op.SUM){
			return 0;
		}
		if(what == Op.COUNT){
			return 0;
		}
		if(what == Op.MIN){
			return Integer.MAX_VALUE;
		}
		if(what == Op.MAX){
			return Integer.MIN_VALUE;
		}
		if(what == Op.AVG){
			return 0;
		}
		return 0;
	}

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		if (gbfield == Aggregator.NO_GROUPING){ //check for aggregator grouping and if none set the gField to null
			myGbfield = null;
		}else{  // if there is a grouping then we need to get that field and save to myGbfield
			myGbfield = tup.getField(gbfield);
		}
		if (!aggHashmap.containsKey(myGbfield)){ 
			//look through the aggregator hashmap
			aggHashmap.put(myGbfield, traverseObj()); 
			//add the object values (received from traversObj) to the hashmap based on myGbfield key
			total.put(myGbfield, 0);           
		}
		
		int tuplevalue = ((IntField) tup.getField(afield)).getValue(); 
    	int currentvalue = aggHashmap.get(myGbfield);
    	int currenttotal = total.get(myGbfield);
    	int result = currentvalue;
    	
    	//Tests op codes and complete necessary operations
			if(what == Op.SUM){
				total.put(myGbfield, currenttotal+1);
				result = tuplevalue + currentvalue;
			}
			if(what == Op.MIN){
				if (tuplevalue > currentvalue){
    				result = currentvalue;
    			}else{
    				result = tuplevalue;
    			}
			}
			if(what == Op.MAX){
				if (tuplevalue > currentvalue){
    				result = tuplevalue;
    			}else{
    				result = currentvalue;
    			}
			}
			if(what == Op.COUNT){
				result = currentvalue + 1;
			}
			if(what == Op.AVG){
				total.put(myGbfield, currenttotal+1);
    			result = tuplevalue + currentvalue;
			}
    	
    	aggHashmap.put(myGbfield, result); //ad the results to the aggregator Hashmap
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
    	TupleDesc tupledesc = genGbDescriptor();
    	
    	Tuple newTuple;
    	//create the fields of the new tuple based on keys which map group fields from the aggregator hashmap
    	for (Field group : aggHashmap.keySet())
    	{
    		int aggVal;
    		if (what == Op.AVG)
    		{
    			aggVal = aggHashmap.get(group) / total.get(group);
    		}
    		else
    		{
    			aggVal = aggHashmap.get(group);
    		}
    		newTuple = new Tuple(tupledesc);
    		if (gbfield == Aggregator.NO_GROUPING){
    			newTuple.setField(0, new IntField(aggVal));
    		}
    		else {
        		newTuple.setField(0, group);
        		newTuple.setField(1, new IntField(aggVal));    			
    		}
    		//add the new tuple to the hashmap
    		tuples.add(newTuple);
    	}
    	//return iterator
    	return new TupleIterator(tupledesc, tuples);
    
		//throw new UnsupportedOperationException("implement me");
	}
	
	public TupleDesc genGbDescriptor()
    {
    	String[] names; //names and types are expected by TupleDesc
    	Type[] types;
    	if (gbfield == Aggregator.NO_GROUPING)
    	{	
    		names = new String[] {
    				"aggValue"
    				};
    		types = new Type[] {
    				Type.INT_TYPE
    				};
    	}
    	else
    	{
    		names = new String[] {
    				"groupvalue",
    				"aggValue"
    				};
    		types = new Type[] {
    				gbfieldtype,
    				Type.INT_TYPE
    				};
    	}
    	return new TupleDesc(types, names);
    }


}