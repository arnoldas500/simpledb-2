package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import simpledb.Aggregator.Op;

/**
 * A {@code StringAggregator} computes some aggregate value over a set of {@code StringField}s.
 */
public class StringAggregator implements Aggregator {


	protected int gbfield;
	protected Op what;
	protected Type gbfieldType;
	protected int afield;
	protected HashMap<Field, Integer> count;

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

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldType = gbfieldtype;
		this.afield = afield;
		this.what = what;
		assert(what == Op.COUNT);
		count = new HashMap<Field, Integer>();
	}

	protected TupleDesc createTd(){
		String[] name;
		Type[] type;
		if(gbfield == Aggregator.NO_GROUPING){
			name = new String[] {"aggregateValue"};
			type = new Type[] {Type.INT_TYPE};
		}else{
			name = new String[] {"groupValue", "aggregateValue"};
			type = new Type[] {gbfieldType, Type.INT_TYPE};
		}
		return new TupleDesc(type,name);
	}
	
	
	/**
	 * Merges a new tuple into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		Field tuplegField;
		if(gbfieldType == null){
			tuplegField = null;
		}else
			tuplegField = tup.getField(gbfield);
		
		if(!count.containsKey(tuplegField)){
			count.put(tuplegField, 0);
		}
		
		int runningTotal = count.get(tuplegField);
		count.put(tuplegField, runningTotal+1);
	}

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The aggregateVal is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		
		ArrayList<Tuple> t1 = new ArrayList<Tuple>();
		TupleDesc aggTupleDesc = createTd();
		Tuple add;
		Field group = (Field) count.keySet().iterator();
		while(((Iterator<Field>) group).hasNext()){
			int total = count.get(group);
			add = new Tuple(aggTupleDesc);
			if(gbfield == Aggregator.NO_GROUPING)
				add.setField(0, new IntField(total));
			else{
				add.setField(0, group);
				add.setField(1, new IntField(total));
			}
			t1.add(add);
		}
		return new TupleIterator(aggTupleDesc, t1);
	}
}
		
		/*
		 * Iterator<String> it = list.iterator();
while (it.hasNext()) {
  String string=it.next();
  System.out.println(string);
}
		for(int i=0; i<count.keySet().size(); i++){
			int total = count.get(i);
			add = new Tuple(aggTupleDesc);
			if(gbfield == Aggregator.NO_GROUPING)
				add.setField(0, new IntField(total));
			else{
				add.setField(0, count.get(i));
			}
			*/
		//}
		//throw new UnsupportedOperationException("implement me");
	//}


